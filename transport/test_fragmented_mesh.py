import asyncio
import unittest
from core.mock_transceiver import MockMedium, MockTransceiver
from transport.fragmented_mesh import FragmentedMeshProtocol


class TestFragmentedMeshNetwork(unittest.IsolatedAsyncioTestCase):

  def setUp(self):
    self.medium = MockMedium(max_range_m=3000, bytes_per_sec=10000)

  def _create_fragmented_node(self, x, y, name, mobile=False):
    tx = MockTransceiver(self.medium, x=x, y=y, name=name)
    proto = FragmentedMeshProtocol(tx, name, mobile=mobile)

    received = []

    def on_msg(sender, payload):
      received.append((sender, payload))

    proto.set_message_callback(on_msg)
    return proto, received

  async def test_fragmentation_direct(self):
    original_size = FragmentedMeshProtocol.FRAGMENT_SIZE
    FragmentedMeshProtocol.FRAGMENT_SIZE = 100
    try:
      proto_a, rec_a = self._create_fragmented_node(0, 0, "Node_A")
      proto_b, rec_b = self._create_fragmented_node(1000, 0, "Node_B")

      large_payload = b"A" * 250

      success = await proto_a.send_message("Node_B", large_payload, timeout=5.0)
      self.assertTrue(
          success, "Fragmented message should be delivered and ACKed")

      await asyncio.sleep(0.1)

      self.assertEqual(len(rec_b), 1)
      self.assertEqual(rec_b[0][0], "Node_A")
      self.assertEqual(rec_b[0][1], large_payload)

      proto_a.close()
      proto_b.close()
    finally:
      FragmentedMeshProtocol.FRAGMENT_SIZE = original_size

  async def test_fragmentation_multihop(self):
    original_size = FragmentedMeshProtocol.FRAGMENT_SIZE
    FragmentedMeshProtocol.FRAGMENT_SIZE = 100
    try:
      proto_a, rec_a = self._create_fragmented_node(0, 0, "Node_A")
      proto_b, rec_b = self._create_fragmented_node(2000, 0, "Node_B")
      proto_c, rec_c = self._create_fragmented_node(4000, 0, "Node_C")

      large_payload = b"A" * 250

      success = await proto_a.send_message("Node_C", large_payload, timeout=8.0)
      self.assertTrue(success, "Fragmented message should traverse hops")

      await asyncio.sleep(0.1)

      self.assertEqual(len(rec_c), 1)
      self.assertEqual(rec_c[0][1], large_payload)
      self.assertEqual(len(rec_b), 0)

      proto_a.close()
      proto_b.close()
      proto_c.close()
    finally:
      FragmentedMeshProtocol.FRAGMENT_SIZE = original_size


if __name__ == "__main__":
  unittest.main()
