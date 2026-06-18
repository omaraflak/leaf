import asyncio
import logging

from core.mesh import Mesh

logger = logging.getLogger("leaf.tcp_tunnel")


class TcpMeshTunnel:
  """Bridges a local TCP port to a remote mesh node.

  Starts a TCP server on a given port. When a client connects, all data
  received from the TCP socket is forwarded over the mesh to the remote
  node, and all data received from the mesh (from that remote node) is
  written back to the TCP socket.

  Uses the base Mesh layer directly instead of FragmentedMesh since TCP
  is a byte stream and does not need message reassembly. Each TCP read
  chunk is sent as a single mesh frame.

  Only one TCP client connection is active at a time.
  """

  CHUNK_SIZE = 200

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
    logger.info("TCP tunnel listening on %s:%d", addr[0], addr[1])

  def close(self):
    """Closes the tunnel, disconnecting any active TCP client."""
    self.mesh.remove_message_listener(self._on_mesh_message)
    self._close_tcp_client()
    if self._server:
      self._server.close()
      self._server = None

  def _close_tcp_client(self):
    """Closes the current TCP client connection."""
    if self._read_task and not self._read_task.done():
      self._read_task.cancel()
      self._read_task = None
    if self._writer:
      self._writer.close()
      self._writer = None

  async def _on_tcp_connect(
      self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
  ):
    """Called when a new TCP client connects."""
    peer = writer.get_extra_info("peername")

    # Reject if there is already an active connection
    if self._writer is not None:
      logger.warning(
          "Rejected TCP connection from %s (already connected)", peer)
      writer.close()
      return

    logger.info("TCP client connected: %s", peer)
    self._writer = writer
    self._read_task = asyncio.create_task(self._tcp_read_loop(reader))

  async def _tcp_read_loop(self, reader: asyncio.StreamReader):
    """Reads data from the TCP client and sends it over the mesh."""
    try:
      while True:
        data = await reader.read(self.CHUNK_SIZE)
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
    """Called when a message is received from the mesh."""
    if sender_id != self.remote_node_id:
      return
    if self._writer is None:
      logger.warning("Received mesh data but no TCP client is connected")
      return
    try:
      self._writer.write(data)
    except Exception:
      logger.exception("Error writing mesh data to TCP client")
