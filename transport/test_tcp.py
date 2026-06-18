import asyncio
import logging
import unittest

from core.mock_transceiver import MockMedium, MockTransceiver
from core.mesh import Mesh
from transport.tcp import MeshTcpServer, MeshTcpClient


class TestMeshTcpServer(unittest.IsolatedAsyncioTestCase):
  """Tests for listen-mode (MeshTcpServer)."""

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

    server_a = MeshTcpServer(mesh_a, "NodeB", tcp_port=0)
    server_b = MeshTcpServer(mesh_b, "NodeA", tcp_port=0)

    await server_a.start()
    await server_b.start()

    port_a = server_a._server.sockets[0].getsockname()[1]
    port_b = server_b._server.sockets[0].getsockname()[1]

    reader_a, writer_a = await asyncio.open_connection("127.0.0.1", port_a)
    await asyncio.sleep(0.1)
    reader_b, writer_b = await asyncio.open_connection("127.0.0.1", port_b)
    await asyncio.sleep(0.1)

    # A → mesh → B
    writer_a.write(b"Hello from A")
    await writer_a.drain()
    await asyncio.sleep(0.5)
    data_b = await asyncio.wait_for(reader_b.read(4096), timeout=2.0)
    self.assertEqual(data_b, b"Hello from A")

    # B → mesh → A
    writer_b.write(b"Hello from B")
    await writer_b.drain()
    await asyncio.sleep(0.5)
    data_a = await asyncio.wait_for(reader_a.read(4096), timeout=2.0)
    self.assertEqual(data_a, b"Hello from B")

    writer_a.close()
    writer_b.close()
    server_a.close()
    server_b.close()

  async def test_rejects_second_connection(self):
    mesh_a = self._create_node("NodeA", 0, 0)
    server = MeshTcpServer(mesh_a, "NodeB", tcp_port=0)
    await server.start()

    port = server._server.sockets[0].getsockname()[1]

    reader1, writer1 = await asyncio.open_connection("127.0.0.1", port)
    await asyncio.sleep(0.1)
    self.assertIsNotNone(server._writer)

    reader2, writer2 = await asyncio.open_connection("127.0.0.1", port)
    await asyncio.sleep(0.1)
    data = await reader2.read(4096)
    self.assertEqual(data, b"")

    writer1.close()
    writer2.close()
    server.close()

  async def test_client_disconnect_allows_reconnect(self):
    mesh_a = self._create_node("NodeA", 0, 0)
    server = MeshTcpServer(mesh_a, "NodeB", tcp_port=0)
    await server.start()

    port = server._server.sockets[0].getsockname()[1]

    reader1, writer1 = await asyncio.open_connection("127.0.0.1", port)
    await asyncio.sleep(0.1)
    self.assertIsNotNone(server._writer)

    writer1.close()
    await asyncio.sleep(0.2)

    reader2, writer2 = await asyncio.open_connection("127.0.0.1", port)
    await asyncio.sleep(0.1)
    self.assertIsNotNone(server._writer)

    writer2.close()
    server.close()

  async def test_large_payload(self):
    mesh_a = self._create_node("NodeA", 0, 0)
    mesh_b = self._create_node("NodeB", 100, 0)

    server_a = MeshTcpServer(mesh_a, "NodeB", tcp_port=0)
    server_b = MeshTcpServer(mesh_b, "NodeA", tcp_port=0)

    await server_a.start()
    await server_b.start()

    port_a = server_a._server.sockets[0].getsockname()[1]
    port_b = server_b._server.sockets[0].getsockname()[1]

    reader_a, writer_a = await asyncio.open_connection("127.0.0.1", port_a)
    await asyncio.sleep(0.1)
    reader_b, writer_b = await asyncio.open_connection("127.0.0.1", port_b)
    await asyncio.sleep(0.1)

    large_payload = b"X" * 2000
    writer_a.write(large_payload)
    await writer_a.drain()

    received = b""
    while len(received) < len(large_payload):
      chunk = await asyncio.wait_for(reader_b.read(4096), timeout=5.0)
      if not chunk:
        break
      received += chunk

    self.assertEqual(received, large_payload)

    writer_a.close()
    writer_b.close()
    server_a.close()
    server_b.close()


class TestMeshTcpClient(unittest.IsolatedAsyncioTestCase):
  """Tests for connect-mode (MeshTcpClient)."""

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

  async def test_http_like_flow(self):
    """Browser → MeshTcpServer → mesh → MeshTcpClient → HTTP server."""
    mesh_a = self._create_node("NodeA", 0, 0)
    mesh_b = self._create_node("NodeB", 100, 0)

    # Fake HTTP server on Node B
    http_received = []

    async def handle_http(reader, writer):
      data = await reader.read(4096)
      http_received.append(data)
      writer.write(b"HTTP/1.1 200 OK\r\n\r\nHello from server!")
      await writer.drain()
      writer.close()

    http_server = await asyncio.start_server(handle_http, "127.0.0.1", 0)
    http_port = http_server.sockets[0].getsockname()[1]

    # Browser side
    server = MeshTcpServer(mesh_a, "NodeB", tcp_port=0)
    # HTTP server side
    client = MeshTcpClient(mesh_b, "NodeA", tcp_port=http_port)

    await server.start()

    # Browser connects
    listen_port = server._server.sockets[0].getsockname()[1]
    reader, writer = await asyncio.open_connection("127.0.0.1", listen_port)

    # Browser sends HTTP request
    writer.write(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
    await writer.drain()
    await asyncio.sleep(1.0)

    response = await asyncio.wait_for(reader.read(4096), timeout=5.0)
    self.assertIn(b"200 OK", response)
    self.assertIn(b"Hello from server!", response)
    self.assertEqual(len(http_received), 1)
    self.assertIn(b"GET / HTTP/1.1", http_received[0])

    writer.close()
    server.close()
    client.close()
    http_server.close()

  async def test_reuses_connection(self):
    """MeshTcpClient keeps the TCP connection open across sessions."""
    mesh_a = self._create_node("NodeA", 0, 0)
    mesh_b = self._create_node("NodeB", 100, 0)

    connection_count = [0]

    async def handle_echo(reader, writer):
      connection_count[0] += 1
      while True:
        data = await reader.read(4096)
        if not data:
          break
        writer.write(data)
        await writer.drain()
      writer.close()

    echo_server = await asyncio.start_server(handle_echo, "127.0.0.1", 0)
    echo_port = echo_server.sockets[0].getsockname()[1]

    server = MeshTcpServer(mesh_a, "NodeB", tcp_port=0)
    client = MeshTcpClient(mesh_b, "NodeA", tcp_port=echo_port)

    await server.start()
    listen_port = server._server.sockets[0].getsockname()[1]

    # First session
    reader1, writer1 = await asyncio.open_connection("127.0.0.1", listen_port)
    writer1.write(b"session1")
    await writer1.drain()
    await asyncio.sleep(0.5)
    data = await asyncio.wait_for(reader1.read(4096), timeout=2.0)
    self.assertEqual(data, b"session1")
    writer1.close()
    await asyncio.sleep(0.3)

    # Second session
    reader2, writer2 = await asyncio.open_connection("127.0.0.1", listen_port)
    writer2.write(b"session2")
    await writer2.drain()
    await asyncio.sleep(0.5)
    data = await asyncio.wait_for(reader2.read(4096), timeout=2.0)
    self.assertEqual(data, b"session2")
    writer2.close()

    self.assertEqual(connection_count[0], 1)

    server.close()
    client.close()
    echo_server.close()


if __name__ == "__main__":
  unittest.main()
