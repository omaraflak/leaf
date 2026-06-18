import asyncio
import json
import logging
import unittest

from core.mock_transceiver import MockMedium, MockTransceiver
from transport.fragmented_mesh import FragmentedMesh
from transport.rpc import Message, MeshRpcClient, MeshRpcServer, register


# Concrete Message classes using JSON serialization
class JsonRequest(Message):

  def __init__(self, **kwargs):
    self.data = kwargs

  def serialize(self) -> bytes:
    # Safe deserialization is enforced by using json.dumps/loads and checking type mapping
    return json.dumps(self.data).encode("utf-8")

  @classmethod
  def deserialize(cls, data: bytes) -> "JsonRequest":
    try:
      parsed = json.loads(data.decode("utf-8"))
      if not isinstance(parsed, dict):
        raise TypeError("Request must be a dictionary")
      return cls(**parsed)
    except Exception as e:
      raise ValueError(f"Invalid message format: {e}")


class JsonResponse(Message):

  def __init__(self, **kwargs):
    self.data = kwargs

  def serialize(self) -> bytes:
    return json.dumps(self.data).encode("utf-8")

  @classmethod
  def deserialize(cls, data: bytes) -> "JsonResponse":
    try:
      parsed = json.loads(data.decode("utf-8"))
      if not isinstance(parsed, dict):
        raise TypeError("Response must be a dictionary")
      return cls(**parsed)
    except Exception as e:
      raise ValueError(f"Invalid message format: {e}")


# Sample Server
class SampleServer(MeshRpcServer):

  @register
  async def add(self, request: JsonRequest) -> JsonResponse:
    a = request.data.get("a", 0)
    b = request.data.get("b", 0)
    return JsonResponse(result=a + b)

  @register
  async def fail_method(self, request: JsonRequest) -> JsonResponse:
    raise ValueError("intentional error")

  @register
  async def slow_method(self, request: JsonRequest) -> JsonResponse:
    delay = request.data.get("delay", 1.0)
    await asyncio.sleep(delay)
    return JsonResponse(status="done")


class TestMeshRpc(unittest.IsolatedAsyncioTestCase):

  def setUp(self):
    logging.disable(logging.CRITICAL)
    self.medium = MockMedium(max_range_m=3000, bytes_per_sec=50000)
    self.nodes = []

  def tearDown(self):
    logging.disable(logging.NOTSET)
    for node in self.nodes:
      node.close()

  def _create_node(self, name: str, x: float, y: float) -> FragmentedMesh:
    tx = MockTransceiver(self.medium, x=x, y=y, name=name)
    mesh = FragmentedMesh(tx, name)
    self.nodes.append(mesh)
    return mesh

  async def test_basic_rpc_call(self):
    server_mesh = self._create_node("ServerNode", 0, 0)
    client_mesh = self._create_node("ClientNode", 100, 0)

    server = SampleServer(server_mesh)

    client = MeshRpcClient(client_mesh, "ServerNode")

    req = JsonRequest(a=10, b=20)
    resp = await client.call("add", req, JsonResponse)

    self.assertEqual(resp.data.get("result"), 30)

    server.close()
    client.close()

  async def test_concurrent_rpc_calls(self):
    server_mesh = self._create_node("ServerNode", 0, 0)
    client_mesh = self._create_node("ClientNode", 100, 0)

    server = SampleServer(server_mesh)

    client = MeshRpcClient(client_mesh, "ServerNode")

    # Launch 5 concurrent calls
    async def call_and_verify(a, b):
      req = JsonRequest(a=a, b=b)
      resp = await client.call("add", req, JsonResponse)
      self.assertEqual(resp.data.get("result"), a + b)

    tasks = [
        asyncio.create_task(call_and_verify(i, i * 2)) for i in range(1, 6)
    ]
    await asyncio.gather(*tasks)

    server.close()
    client.close()

  async def test_invalid_method(self):
    server_mesh = self._create_node("ServerNode", 0, 0)
    client_mesh = self._create_node("ClientNode", 100, 0)

    server = SampleServer(server_mesh)

    client = MeshRpcClient(client_mesh, "ServerNode")

    req = JsonRequest()
    with self.assertRaises(RuntimeError) as ctx:
      await client.call("non_existent", req, JsonResponse)

    self.assertIn("Method 'non_existent' not found", str(ctx.exception))

    server.close()
    client.close()

  async def test_method_exception(self):
    server_mesh = self._create_node("ServerNode", 0, 0)
    client_mesh = self._create_node("ClientNode", 100, 0)

    server = SampleServer(server_mesh)

    client = MeshRpcClient(client_mesh, "ServerNode")

    req = JsonRequest()
    with self.assertRaises(RuntimeError) as ctx:
      await client.call("fail_method", req, JsonResponse)

    self.assertIn("intentional error", str(ctx.exception))

    server.close()
    client.close()

  async def test_client_timeout(self):
    server_mesh = self._create_node("ServerNode", 0, 0)
    client_mesh = self._create_node("ClientNode", 100, 0)

    server = SampleServer(server_mesh)

    client = MeshRpcClient(client_mesh, "ServerNode")

    req = JsonRequest(delay=2.0)
    # Set timeout shorter than delay
    with self.assertRaises(TimeoutError):
      await client.call("slow_method", req, JsonResponse, timeout=0.5)

    server.close()
    client.close()

  async def test_coexistence_and_chaining(self):
    # Test client and server on the same node
    node_mesh = self._create_node("NodeA", 0, 0)
    other_mesh = self._create_node("NodeB", 100, 0)

    # Server on NodeB
    server_b = SampleServer(other_mesh)

    # Server on NodeA
    server_a = SampleServer(node_mesh)

    # Client on NodeA talking to NodeB
    client_a = MeshRpcClient(node_mesh, "NodeB")

    # Regular non-RPC mesh callback registered on NodeA
    mesh_received = []

    def on_raw_msg(sender, payload):
      mesh_received.append((sender, payload))

    # This should wrap / coexist with the RPC server/client callbacks
    # Since we set callback *after* server/client are created, we need to make sure
    # they don't break. Or if we register it before:
    # Let's test both directions.
    node_mesh.add_message_listener(on_raw_msg)

    # Now let's try calling ServerB from ClientA
    req = JsonRequest(a=5, b=5)
    resp = await client_a.call("add", req, JsonResponse)
    self.assertEqual(resp.data.get("result"), 10)

    # With the multi-listener model, the raw listener sees all messages
    # (including RPC traffic). Verify the raw test message was received.
    await other_mesh.send_message("NodeA", b"raw test message")
    await asyncio.sleep(0.2)

    raw_messages = [(s, p)
                    for s, p in mesh_received if p == b"raw test message"]
    self.assertEqual(len(raw_messages), 1)
    self.assertEqual(raw_messages[0][0], "NodeB")

    server_a.close()
    server_b.close()
    client_a.close()


if __name__ == "__main__":
  unittest.main()
