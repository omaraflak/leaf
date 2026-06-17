import unittest
import time
import threading
from mock_transceiver import MockMedium, MockTransceiver
from mesh_protocol import MeshProtocol


class TestMeshNetwork(unittest.TestCase):
  def setUp(self):
    # Create a fast medium for testing (10000 bytes/sec means very short delays)
    self.medium = MockMedium(max_range_m=3000, bytes_per_sec=10000)

  def _create_node(self, x, y, name):
    tx = MockTransceiver(self.medium, x=x, y=y, name=name)
    proto = MeshProtocol(tx, name)

    # Track received messages
    received = []

    def on_msg(sender, payload):
      received.append((sender, payload))
    proto.set_message_callback(on_msg)

    return proto, received

  def test_direct_communication(self):
    # A and B are close to each other
    proto_a, rec_a = self._create_node(0, 0, "Node_A")
    proto_b, rec_b = self._create_node(1000, 0, "Node_B")

    # Use a larger timeout for AODV discovery
    success = proto_a.send_message("Node_B", b"Hello B", timeout=2.0)
    self.assertTrue(success, "Message should be acknowledged")

    self.assertEqual(len(rec_b), 1)
    self.assertEqual(rec_b[0][0], "Node_A")
    self.assertEqual(rec_b[0][1], b"Hello B")

  def test_out_of_range(self):
    # A and C are out of range
    proto_a, rec_a = self._create_node(0, 0, "Node_A")
    proto_c, rec_c = self._create_node(4000, 0, "Node_C")

    # Use low timeout for quick failure
    success = proto_a.send_message(
        "Node_C", b"Hello C", timeout=0.5, max_retries=0)
    self.assertFalse(success, "Message should fail to be delivered")

    self.assertEqual(len(rec_c), 0)

  def test_multi_hop(self):
    # A -> B -> C
    proto_a, rec_a = self._create_node(0, 0, "Node_A")
    proto_b, rec_b = self._create_node(2000, 0, "Node_B")
    proto_c, rec_c = self._create_node(4000, 0, "Node_C")

    success = proto_a.send_message("Node_C", b"Multi-hop message", timeout=5.0)
    self.assertTrue(success, "Multi-hop message should be acknowledged")

    self.assertEqual(len(rec_c), 1)
    self.assertEqual(rec_c[0][0], "Node_A")
    self.assertEqual(rec_c[0][1], b"Multi-hop message")

    # B shouldn't trigger its on_message_callback since it wasn't the destination
    self.assertEqual(len(rec_b), 0)

  def test_route_caching(self):
    # A -> B -> C
    proto_a, rec_a = self._create_node(0, 0, "Node_A")
    proto_b, rec_b = self._create_node(2000, 0, "Node_B")
    proto_c, rec_c = self._create_node(4000, 0, "Node_C")

    # First message takes longer due to route discovery
    success1 = proto_a.send_message("Node_C", b"Message 1", timeout=5.0)
    self.assertTrue(success1)

    # Second message should be faster as route is cached
    import time
    start = time.time()
    success2 = proto_a.send_message("Node_C", b"Message 2", timeout=5.0)
    duration = time.time() - start
    self.assertTrue(success2)
    # Should not need to wait for RREQ/RREP
    self.assertTrue(
        duration < 1.0, f"Cached route should be fast, took {duration}s")

    self.assertEqual(len(rec_c), 2)

  def test_broken_link(self):
    # A -> B -> C
    proto_a, rec_a = self._create_node(0, 0, "Node_A")
    proto_b, rec_b = self._create_node(2000, 0, "Node_B")
    proto_c, rec_c = self._create_node(4000, 0, "Node_C")

    # 1. Establish route and send message
    success = proto_a.send_message("Node_C", b"Msg 1", timeout=5.0)
    self.assertTrue(success)

    # 2. Break the link (move B out of range)
    proto_b.transceiver.x = 10000

    # 3. Message should fail and route should be cleared
    success2 = proto_a.send_message("Node_C", b"Msg 2", timeout=3.0)
    self.assertFalse(success2)

  def test_broadcast(self):
    proto_a, rec_a = self._create_node(0, 0, "Node_A")
    proto_b, rec_b = self._create_node(1000, 0, "Node_B")
    proto_c, rec_c = self._create_node(2000, 0, "Node_C")

    proto_a.broadcast_message(b"Broadcast!")
    time.sleep(0.2)

    self.assertEqual(len(rec_b), 1)
    self.assertEqual(rec_b[0][1], b"Broadcast!")

    self.assertEqual(len(rec_c), 1)
    self.assertEqual(rec_c[0][1], b"Broadcast!")

  def test_deduplication(self):
    # A sends to B. We hack A to send the exact same sequence number twice.
    proto_a, rec_a = self._create_node(0, 0, "Node_A")
    proto_b, rec_b = self._create_node(1000, 0, "Node_B")

    # Use broadcast directly to simulate duplicate packets
    proto_a.broadcast_message(b"Duplicate me")
    time.sleep(0.1)

    proto_a.seq_num -= 1  # Reset it back to send same seq
    proto_a.broadcast_message(b"Duplicate me")
    time.sleep(0.1)

    # B should have received it only once
    self.assertEqual(len(rec_b), 1)

  def test_csma_ca_avoids_collision(self):
    # Test CSMA/CA: A and C both send to B. With Listen Before Talk,
    # the second sender should detect the first's signal and back off.
    # Use a slow medium so transmissions last long enough to detect.
    # Use TTL=1 to prevent B from rebroadcasting (which would cause
    # secondary collisions unrelated to CSMA/CA).
    self.medium.bytes_per_sec = 100

    proto_a, rec_a = self._create_node(0, 0, "Node_A")
    proto_b, rec_b = self._create_node(1000, 0, "Node_B")
    proto_c, rec_c = self._create_node(2000, 0, "Node_C")

    from frame import FrameType

    def send_ttl1(proto, payload):
      """Send a single-hop broadcast (TTL=1, no rebroadcast)."""
      with proto.lock:
        seq = proto.seq_num
        proto.seq_num += 1
      proto._transmit_raw_frame(
          FrameType.DATA, 1, seq, proto.node_id,
          proto.BROADCAST_MAC, proto.node_id,
          proto.BROADCAST_MAC, payload,
      )

    # A starts first, C starts 50ms later (enough time to detect A's signal)
    t1 = threading.Thread(target=send_ttl1, args=(proto_a, b"A message"))
    t1.start()
    time.sleep(0.05)
    t2 = threading.Thread(target=send_ttl1, args=(proto_c, b"C message"))
    t2.start()

    t1.join()
    t2.join()

    # Wait for slow transmissions and CSMA backoffs to finish
    time.sleep(3.0)

    # B should receive BOTH messages cleanly because CSMA/CA staggered them
    self.assertEqual(len(
        rec_b), 2, "CSMA/CA should have prevented the collision, allowing B to receive both.")

    # rec_b should contain one message from A and one from C
    senders = {msg[0] for msg in rec_b}
    self.assertIn("Node_A", senders)
    self.assertIn("Node_C", senders)


if __name__ == '__main__':
  unittest.main()
