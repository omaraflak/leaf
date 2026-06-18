import asyncio
import logging
import unittest

from core.mock_transceiver import MockMedium, MockTransceiver
from core.mesh import Mesh
from transport.tcp_tunnel import TcpMeshTunnel


class TestTcpMeshTunnel(unittest.IsolatedAsyncioTestCase):

  def setUp(self):
    logging.disable(logging.CRITICAL)
    self.medium = MockMedium(max_range_m=3000, bytes_per_sec=50000)
    self.nodes = []

  def tearDown(self):
    logging.disable(logging.NOTSET)
    for node in self.nodes:
      node.close()

  def _create_node(self, name: str, x: float, y: float) -> Mesh:
    tx = MockTransceiver(self.medium, x=x, y=y, name=name)
    mesh = Mesh(tx, name)
    self.nodes.append(mesh)
    return mesh

  async def test_bidirectional_tunnel(self):
    mesh_a = self._create_node("NodeA", 0, 0)
    mesh_b = self._create_node("NodeB", 100, 0)

    tunnel_a = TcpMeshTunnel(mesh_a, "NodeB", tcp_port=0)
    tunnel_b = TcpMeshTunnel(mesh_b, "NodeA", tcp_port=0)

    await tunnel_a.start()
    await tunnel_b.start()

    port_a = tunnel_a._server.sockets[0].getsockname()[1]
    port_b = tunnel_b._server.sockets[0].getsockname()[1]

    # Connect TCP clients to each tunnel
    reader_a, writer_a = await asyncio.open_connection("127.0.0.1", port_a)
    await asyncio.sleep(0.1)

    reader_b, writer_b = await asyncio.open_connection("127.0.0.1", port_b)
    await asyncio.sleep(0.1)

    # Send from TCP client A → mesh → TCP client B
    writer_a.write(b"Hello from A")
    await writer_a.drain()
    await asyncio.sleep(0.5)

    data_b = await asyncio.wait_for(reader_b.read(4096), timeout=2.0)
    self.assertEqual(data_b, b"Hello from A")

    # Send from TCP client B → mesh → TCP client A
    writer_b.write(b"Hello from B")
    await writer_b.drain()
    await asyncio.sleep(0.5)

    data_a = await asyncio.wait_for(reader_a.read(4096), timeout=2.0)
    self.assertEqual(data_a, b"Hello from B")

    writer_a.close()
    writer_b.close()
    tunnel_a.close()
    tunnel_b.close()

  async def test_rejects_second_connection(self):
    mesh_a = self._create_node("NodeA", 0, 0)
    tunnel = TcpMeshTunnel(mesh_a, "NodeB", tcp_port=0)
    await tunnel.start()

    port = tunnel._server.sockets[0].getsockname()[1]

    # First connection succeeds
    reader1, writer1 = await asyncio.open_connection("127.0.0.1", port)
    await asyncio.sleep(0.1)
    self.assertIsNotNone(tunnel._writer)

    # Second connection gets rejected
    reader2, writer2 = await asyncio.open_connection("127.0.0.1", port)
    await asyncio.sleep(0.1)

    # The second connection should be closed by the server
    data = await reader2.read(4096)
    self.assertEqual(data, b"")  # EOF = connection closed

    writer1.close()
    writer2.close()
    tunnel.close()

  async def test_client_disconnect_allows_reconnect(self):
    mesh_a = self._create_node("NodeA", 0, 0)
    tunnel = TcpMeshTunnel(mesh_a, "NodeB", tcp_port=0)
    await tunnel.start()

    port = tunnel._server.sockets[0].getsockname()[1]

    # First connection
    reader1, writer1 = await asyncio.open_connection("127.0.0.1", port)
    await asyncio.sleep(0.1)
    self.assertIsNotNone(tunnel._writer)

    # Disconnect
    writer1.close()
    await asyncio.sleep(0.2)

    # Should be able to reconnect
    reader2, writer2 = await asyncio.open_connection("127.0.0.1", port)
    await asyncio.sleep(0.1)
    self.assertIsNotNone(tunnel._writer)

    writer2.close()
    tunnel.close()

  async def test_large_payload_through_tunnel(self):
    mesh_a = self._create_node("NodeA", 0, 0)
    mesh_b = self._create_node("NodeB", 100, 0)

    tunnel_a = TcpMeshTunnel(mesh_a, "NodeB", tcp_port=0)
    tunnel_b = TcpMeshTunnel(mesh_b, "NodeA", tcp_port=0)

    await tunnel_a.start()
    await tunnel_b.start()

    port_a = tunnel_a._server.sockets[0].getsockname()[1]
    port_b = tunnel_b._server.sockets[0].getsockname()[1]

    reader_a, writer_a = await asyncio.open_connection("127.0.0.1", port_a)
    await asyncio.sleep(0.1)

    reader_b, writer_b = await asyncio.open_connection("127.0.0.1", port_b)
    await asyncio.sleep(0.1)

    # Send a payload larger than CHUNK_SIZE (will be split across
    # multiple mesh messages and reassembled by the TCP stream)
    large_payload = b"X" * 2000
    writer_a.write(large_payload)
    await writer_a.drain()

    # Read all data on the other side
    received = b""
    while len(received) < len(large_payload):
      chunk = await asyncio.wait_for(reader_b.read(4096), timeout=5.0)
      if not chunk:
        break
      received += chunk

    self.assertEqual(received, large_payload)

    writer_a.close()
    writer_b.close()
    tunnel_a.close()
    tunnel_b.close()


if __name__ == "__main__":
  unittest.main()
