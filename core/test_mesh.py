import asyncio
import logging
import time
import unittest
from core.frame import FrameType, MeshFrame
from core.mock_transceiver import MockMedium, MockTransceiver
from core.mesh import Mesh


class TestMeshNetwork(unittest.IsolatedAsyncioTestCase):

  def setUp(self):
    logging.disable(logging.CRITICAL)
    # Create a fast medium for testing (10000 bytes/sec means very short delays)
    self.medium = MockMedium(max_range_m=3000, bytes_per_sec=10000)

  def tearDown(self):
    logging.disable(logging.NOTSET)

  def _create_node(self, x, y, name, mobile=False):
    tx = MockTransceiver(self.medium, x=x, y=y, name=name)
    proto = Mesh(tx, name, mobile=mobile)

    # Track received messages
    received = []

    def on_msg(sender, payload):
      received.append((sender, payload))

    proto.set_message_callback(on_msg)

    return proto, received

  async def test_direct_communication(self):
    # A and B are close to each other
    proto_a, rec_a = self._create_node(0, 0, "Node_A")
    proto_b, rec_b = self._create_node(1000, 0, "Node_B")

    success = await proto_a.send_message("Node_B", b"Hello B", timeout=2.0)
    self.assertTrue(success, "Message should be acknowledged")

    self.assertEqual(len(rec_b), 1)
    self.assertEqual(rec_b[0][0], "Node_A")
    self.assertEqual(rec_b[0][1], b"Hello B")

  async def test_out_of_range(self):
    # A and C are out of range
    proto_a, rec_a = self._create_node(0, 0, "Node_A")
    proto_c, rec_c = self._create_node(4000, 0, "Node_C")

    success = await proto_a.send_message(
        "Node_C", b"Hello C", timeout=0.5, max_retries=0
    )
    self.assertFalse(success, "Message should fail to be delivered")

    self.assertEqual(len(rec_c), 0)

  async def test_multi_hop(self):
    # A -> B -> C
    proto_a, rec_a = self._create_node(0, 0, "Node_A")
    proto_b, rec_b = self._create_node(2000, 0, "Node_B")
    proto_c, rec_c = self._create_node(4000, 0, "Node_C")

    success = await proto_a.send_message(
        "Node_C", b"Multi-hop message", timeout=5.0
    )
    self.assertTrue(success, "Multi-hop message should be acknowledged")

    self.assertEqual(len(rec_c), 1)
    self.assertEqual(rec_c[0][0], "Node_A")
    self.assertEqual(rec_c[0][1], b"Multi-hop message")

    # B shouldn't trigger its on_message_callback since it wasn't the destination
    self.assertEqual(len(rec_b), 0)

  async def test_route_caching(self):
    # A -> B -> C
    proto_a, rec_a = self._create_node(0, 0, "Node_A")
    proto_b, rec_b = self._create_node(2000, 0, "Node_B")
    proto_c, rec_c = self._create_node(4000, 0, "Node_C")

    # First message takes longer due to route discovery
    success1 = await proto_a.send_message("Node_C", b"Message 1", timeout=5.0)
    self.assertTrue(success1)

    # Second message should be faster as route is cached
    start = time.time()
    success2 = await proto_a.send_message("Node_C", b"Message 2", timeout=5.0)
    duration = time.time() - start
    self.assertTrue(success2)
    # Should not need to wait for RREQ/RREP
    self.assertTrue(
        duration < 1.0, f"Cached route should be fast, took {duration}s"
    )

    self.assertEqual(len(rec_c), 2)

  async def test_broken_link(self):
    # A -> B -> C
    proto_a, rec_a = self._create_node(0, 0, "Node_A")
    proto_b, rec_b = self._create_node(2000, 0, "Node_B")
    proto_c, rec_c = self._create_node(4000, 0, "Node_C")

    # 1. Establish route and send message
    success = await proto_a.send_message("Node_C", b"Msg 1", timeout=5.0)
    self.assertTrue(success)

    # 2. Break the link (move B out of range)
    proto_b.transceiver.x = 10000

    # 3. Message should fail and route should be cleared
    success2 = await proto_a.send_message("Node_C", b"Msg 2", timeout=3.0)
    self.assertFalse(success2)

  async def test_broadcast(self):
    proto_a, rec_a = self._create_node(0, 0, "Node_A")
    proto_b, rec_b = self._create_node(1000, 0, "Node_B")
    proto_c, rec_c = self._create_node(2000, 0, "Node_C")

    await proto_a.broadcast_message(b"Broadcast!")
    await asyncio.sleep(0.2)

    self.assertEqual(len(rec_b), 1)
    self.assertEqual(rec_b[0][1], b"Broadcast!")

    self.assertEqual(len(rec_c), 1)
    self.assertEqual(rec_c[0][1], b"Broadcast!")

  async def test_deduplication(self):
    # A sends to B. We hack A to send the exact same sequence number twice.
    proto_a, rec_a = self._create_node(0, 0, "Node_A")
    proto_b, rec_b = self._create_node(1000, 0, "Node_B")

    # Use broadcast directly to simulate duplicate packets
    await proto_a.broadcast_message(b"Duplicate me")
    await asyncio.sleep(0.1)

    proto_a.seq_num -= 1  # Reset it back to send same seq
    await proto_a.broadcast_message(b"Duplicate me")
    await asyncio.sleep(0.1)

    # B should have received it only once
    self.assertEqual(len(rec_b), 1)

  async def test_csma_ca_avoids_collision(self):
    # Test CSMA/CA: A and C both send to B. With Listen Before Talk,
    # the second sender should detect the first's signal and back off.
    # Use a slow medium so transmissions last long enough to detect.
    # Use TTL=1 to prevent B from rebroadcasting (which would cause
    # secondary collisions unrelated to CSMA/CA).
    self.medium.bytes_per_sec = 100

    proto_a, rec_a = self._create_node(0, 0, "Node_A")
    proto_b, rec_b = self._create_node(1000, 0, "Node_B")
    proto_c, rec_c = self._create_node(2000, 0, "Node_C")

    async def send_ttl1(proto, payload):
      """Send a single-hop broadcast (TTL=1, no rebroadcast)."""
      seq = proto.seq_num
      proto.seq_num += 1
      await proto._transmit_raw_frame(
          FrameType.DATA,
          1,
          seq,
          proto.node_id,
          MeshFrame.BROADCAST_MAC,
          proto.node_id,
          MeshFrame.BROADCAST_MAC,
          payload,
      )

    # A starts first, C starts 50ms later (enough time to detect A's signal)
    task_a = asyncio.create_task(send_ttl1(proto_a, b"A message"))
    await asyncio.sleep(0.05)
    task_c = asyncio.create_task(send_ttl1(proto_c, b"C message"))

    await task_a
    await task_c

    # Wait for slow transmissions and CSMA backoffs to finish
    await asyncio.sleep(3.0)

    # B should receive BOTH messages cleanly because CSMA/CA staggered them
    self.assertEqual(
        len(rec_b),
        2,
        "CSMA/CA should have prevented the collision, allowing B to receive both.",
    )

    # rec_b should contain one message from A and one from C
    senders = {msg[0] for msg in rec_b}
    self.assertIn("Node_A", senders)
    self.assertIn("Node_C", senders)

  async def test_route_discovery_coalescing(self):
    # A -> B -> C
    # We want to measure the number of RREQs broadcast by A.
    # To do this, we intercept self.medium.transmit.
    rreq_count = 0
    original_transmit = self.medium.transmit

    async def intercept_transmit(sender, data):
      nonlocal rreq_count
      frame = MeshFrame.unpack(data)
      if frame and frame.frame_type == FrameType.RREQ and sender.name == "Node_A":
        rreq_count += 1
      await original_transmit(sender, data)

    self.medium.transmit = intercept_transmit

    proto_a, rec_a = self._create_node(0, 0, "Node_A")
    proto_b, rec_b = self._create_node(2000, 0, "Node_B")
    proto_c, rec_c = self._create_node(4000, 0, "Node_C")

    # Send 3 concurrent messages from A to C before route is known
    results = await asyncio.gather(
        proto_a.send_message("Node_C", b"Msg 1", timeout=5.0),
        proto_a.send_message("Node_C", b"Msg 2", timeout=5.0),
        proto_a.send_message("Node_C", b"Msg 3", timeout=5.0),
        return_exceptions=True,
    )

    # Check they were all successfully delivered
    for res in results:
      self.assertTrue(res)

    # Without coalescing, A would transmit 3 RREQs (one for each message).
    # With coalescing, A should only transmit 1 RREQ.
    self.assertEqual(
        rreq_count, 1, f"Expected exactly 1 RREQ broadcast, got {rreq_count}"
    )

  async def test_adaptive_route_expiry(self):
    # Stationary -> stationary: long expiry
    proto_a, _ = self._create_node(0, 0, "Node_A")
    proto_b, _ = self._create_node(1000, 0, "Node_B")

    success = await proto_a.send_message("Node_B", b"hi", timeout=2.0)
    self.assertTrue(success)

    dest_bytes = "Node_B".encode("utf-8")[:8].ljust(8, b"\x00")
    expiry = proto_a.routing_table[dest_bytes][2]
    self.assertAlmostEqual(
        expiry, time.time() + Mesh.ROUTE_EXPIRY_STATIONARY_SEC, delta=5.0
    )

    # Stationary -> mobile: short expiry
    proto_c, _ = self._create_node(0, 0, "Node_C")
    proto_d, _ = self._create_node(1000, 0, "Node_D", mobile=True)

    success = await proto_c.send_message("Node_D", b"hi", timeout=2.0)
    self.assertTrue(success)

    dest_bytes = "Node_D".encode("utf-8")[:8].ljust(8, b"\x00")
    expiry = proto_c.routing_table[dest_bytes][2]
    self.assertAlmostEqual(
        expiry, time.time() + Mesh.ROUTE_EXPIRY_MOBILE_SEC, delta=5.0
    )

  async def test_route_preference_stationary_over_mobile(self):
    proto_a, _ = self._create_node(0, 0, "Node_A")

    # _is_better_route(new_hops, new_mobile, old_hops, old_mobile)
    # Stationary should replace mobile at same hop count
    self.assertTrue(proto_a._is_better_route(2, False, 2, True))
    # Mobile should NOT replace stationary at same hop count
    self.assertFalse(proto_a._is_better_route(2, True, 2, False))
    # Stationary should replace mobile even with 2 extra hops
    self.assertTrue(proto_a._is_better_route(4, False, 2, True))
    # Stationary should NOT replace mobile with 3 extra hops
    self.assertFalse(proto_a._is_better_route(5, False, 2, True))
    # Mobile should replace stationary only if significantly shorter
    self.assertTrue(proto_a._is_better_route(1, True, 4, False))
    self.assertFalse(proto_a._is_better_route(2, True, 4, False))
    # Same class: fewer hops wins
    self.assertTrue(proto_a._is_better_route(2, False, 3, False))
    self.assertFalse(proto_a._is_better_route(3, False, 3, False))

  async def test_payload_limit_errors(self):
    proto_a, _ = self._create_node(0, 0, "Node_A")
    proto_b, _ = self._create_node(1000, 0, "Node_B")

    large_payload = b"X" * 65536

    with self.assertRaises(ValueError):
      await proto_a.send_message("Node_B", large_payload)

    with self.assertRaises(ValueError):
      await proto_a.broadcast_message(large_payload)


if __name__ == "__main__":
  unittest.main()
