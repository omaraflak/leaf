import asyncio
import logging

from core.mesh import Mesh

logger = logging.getLogger("leaf.tcp_tunnel")

_CHUNK_SIZE = 200


class MeshTcpServer:
  """
  Starts a TCP server and bridges connections to a remote mesh node.

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
  """Connects to a local TCP service and bridges it to all remote mesh nodes.

  Use on the side where a service (e.g. an HTTP server) is running.
  When data arrives from the mesh, a TCP connection is lazily opened to
  the local service. Data flows bidirectionally between the mesh and the
  TCP connection.
  """

  def __init__(
      self,
      mesh: Mesh,
      remote_node_id: str | None = None,
      tcp_port: int = 0,
      tcp_host: str = "127.0.0.1",
  ):
    self.mesh = mesh
    self.remote_node_id = remote_node_id
    self.tcp_port = tcp_port
    self.tcp_host = tcp_host

    self._connections: dict[str, dict] = {}
    self._connecting: dict[str, list[bytes]] = {}

    self.mesh.add_message_listener(self._on_mesh_message)

  def close(self):
    """Closes the client and all active TCP connections."""
    self.mesh.remove_message_listener(self._on_mesh_message)
    for sender_id in list(self._connections.keys()):
      self._close_connection(sender_id)

  def _close_connection(self, sender_id: str):
    state = self._connections.pop(sender_id, None)
    if state:
      writer = state.get("writer")
      read_task = state.get("read_task")
      if (
          read_task
          and not read_task.done()
          and asyncio.current_task() != read_task
      ):
        read_task.cancel()
      if writer:
        try:
          writer.close()
        except Exception:
          pass

  async def _connect_to_service(self, sender_id: str):
    try:
      reader, writer = await asyncio.open_connection(self.tcp_host, self.tcp_port)
      logger.info(
          "Connected to local service at %s:%d for %s",
          self.tcp_host,
          self.tcp_port,
          sender_id,
      )

      # Retrieve and write any queued data
      pending_data = self._connecting.pop(sender_id, [])
      for chunk in pending_data:
        writer.write(chunk)

      # Create read loop task
      read_task = asyncio.create_task(self._tcp_read_loop(sender_id, reader))

      self._connections[sender_id] = {
          "writer": writer,
          "read_task": read_task,
      }
    except Exception:
      logger.exception(
          "Failed to connect to local service at %s:%d for %s",
          self.tcp_host,
          self.tcp_port,
          sender_id,
      )
      self._connecting.pop(sender_id, None)

  async def _tcp_read_loop(self, sender_id: str, reader: asyncio.StreamReader):
    try:
      while True:
        data = await reader.read(_CHUNK_SIZE)
        if not data:
          logger.info(
              "TCP connection to local service closed for %s", sender_id
          )
          break
        success = await self.mesh.send_message(sender_id, data)
        if not success:
          logger.warning("Failed to send TCP data over mesh to %s", sender_id)
    except asyncio.CancelledError:
      logger.debug("TCP read loop cancelled for %s", sender_id)
    except Exception:
      logger.exception("Error in TCP read loop for %s", sender_id)
    finally:
      self._close_connection(sender_id)

  def _on_mesh_message(self, sender_id: str, data: bytes):
    if sender_id in self._connections:
      state = self._connections[sender_id]
      try:
        state["writer"].write(data)
      except Exception:
        logger.exception("Error writing mesh data to TCP for %s", sender_id)
    elif sender_id in self._connecting:
      self._connecting[sender_id].append(data)
    else:
      self._connecting[sender_id] = [data]
      asyncio.create_task(self._connect_to_service(sender_id))
