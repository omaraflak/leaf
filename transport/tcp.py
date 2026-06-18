import asyncio
import logging

from core.mesh import Mesh

logger = logging.getLogger("leaf.tcp_tunnel")

_CHUNK_SIZE = 200


class MeshTcpServer:
  """Starts a TCP server and bridges connections to a remote mesh node.

  Use on the side where an application (e.g. a browser) initiates TCP
  connections. Data from the TCP client is forwarded over the mesh, and
  data from the mesh is written back to the TCP client.

  Only one TCP client connection is active at a time.
  """

  def __init__(
      self,
      mesh: Mesh,
      remote_node_id: str,
      tcp_port: int,
      tcp_host: str = "127.0.0.1",
  ):
    self.mesh = mesh
    self.remote_node_id = remote_node_id
    self.tcp_port = tcp_port
    self.tcp_host = tcp_host

    self._server: asyncio.Server | None = None
    self._writer: asyncio.StreamWriter | None = None
    self._read_task: asyncio.Task | None = None

    self.mesh.add_message_listener(self._on_mesh_message)

  async def start(self):
    """Starts the TCP server."""
    self._server = await asyncio.start_server(
        self._on_tcp_connect, self.tcp_host, self.tcp_port
    )
    addr = self._server.sockets[0].getsockname()
    logger.info("MeshTcpServer listening on %s:%d", addr[0], addr[1])

  def close(self):
    """Closes the server and any active TCP connection."""
    self.mesh.remove_message_listener(self._on_mesh_message)
    self._close_tcp_client()
    if self._server:
      self._server.close()
      self._server = None

  def _close_tcp_client(self):
    if self._read_task and not self._read_task.done():
      self._read_task.cancel()
      self._read_task = None
    if self._writer:
      self._writer.close()
      self._writer = None

  async def _on_tcp_connect(
      self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
  ):
    peer = writer.get_extra_info("peername")

    if self._writer is not None:
      logger.warning(
          "Rejected TCP connection from %s (already connected)", peer)
      writer.close()
      return

    logger.info("TCP client connected: %s", peer)
    self._writer = writer
    self._read_task = asyncio.create_task(self._tcp_read_loop(reader))

  async def _tcp_read_loop(self, reader: asyncio.StreamReader):
    try:
      while True:
        data = await reader.read(_CHUNK_SIZE)
        if not data:
          logger.info("TCP client disconnected")
          break
        success = await self.mesh.send_message(self.remote_node_id, data)
        if not success:
          logger.warning("Failed to send TCP data over mesh")
    except asyncio.CancelledError:
      logger.debug("TCP read loop cancelled")
    except Exception:
      logger.exception("Error in TCP read loop")
    finally:
      self._close_tcp_client()

  def _on_mesh_message(self, sender_id: str, data: bytes):
    if sender_id != self.remote_node_id:
      return
    if self._writer is None:
      logger.warning("Received mesh data but no TCP client is connected")
      return
    try:
      self._writer.write(data)
    except Exception:
      logger.exception("Error writing mesh data to TCP client")


class MeshTcpClient:
  """Connects to a local TCP service and bridges it to a remote mesh node.

  Use on the side where a service (e.g. an HTTP server) is running.
  When data arrives from the mesh, a TCP connection is lazily opened to
  the local service. Data flows bidirectionally between the mesh and the
  TCP connection.
  """

  def __init__(
      self,
      mesh: Mesh,
      remote_node_id: str,
      tcp_port: int,
      tcp_host: str = "127.0.0.1",
  ):
    self.mesh = mesh
    self.remote_node_id = remote_node_id
    self.tcp_port = tcp_port
    self.tcp_host = tcp_host

    self._writer: asyncio.StreamWriter | None = None
    self._read_task: asyncio.Task | None = None

    self.mesh.add_message_listener(self._on_mesh_message)

  def close(self):
    """Closes the client and any active TCP connection."""
    self.mesh.remove_message_listener(self._on_mesh_message)
    self._close_tcp()

  def _close_tcp(self):
    if self._read_task and not self._read_task.done():
      self._read_task.cancel()
      self._read_task = None
    if self._writer:
      self._writer.close()
      self._writer = None

  async def _connect_to_service(self):
    try:
      reader, writer = await asyncio.open_connection(self.tcp_host, self.tcp_port)
      logger.info(
          "Connected to local service at %s:%d", self.tcp_host, self.tcp_port
      )
      self._writer = writer
      self._read_task = asyncio.create_task(self._tcp_read_loop(reader))
    except Exception:
      logger.exception(
          "Failed to connect to local service at %s:%d",
          self.tcp_host,
          self.tcp_port,
      )

  async def _tcp_read_loop(self, reader: asyncio.StreamReader):
    try:
      while True:
        data = await reader.read(_CHUNK_SIZE)
        if not data:
          logger.info("TCP connection to local service closed")
          break
        success = await self.mesh.send_message(self.remote_node_id, data)
        if not success:
          logger.warning("Failed to send TCP data over mesh")
    except asyncio.CancelledError:
      logger.debug("TCP read loop cancelled")
    except Exception:
      logger.exception("Error in TCP read loop")
    finally:
      self._close_tcp()

  def _on_mesh_message(self, sender_id: str, data: bytes):
    if sender_id != self.remote_node_id:
      return

    if self._writer is None:
      asyncio.create_task(self._connect_and_write(data))
      return

    try:
      self._writer.write(data)
    except Exception:
      logger.exception("Error writing mesh data to TCP")

  async def _connect_and_write(self, data: bytes):
    await self._connect_to_service()
    if self._writer is not None:
      try:
        self._writer.write(data)
      except Exception:
        logger.exception("Error writing mesh data to TCP after connect")
