import time
import random
import threading
from typing import Callable
from transceiver import Transceiver
from frame import FrameType, MeshFrame


class MeshProtocol:
  """
  A mesh networking protocol built on top of a raw radio Transceiver.
  Implements AODV-like routing, CSMA/CA, and End-to-End ACKs.
  """

  MAGIC = b"\xaa\xbb"
  BROADCAST_MAC = b"\xff" * 8

  def __init__(self, transceiver: Transceiver, node_id: str, channel: int = 0):
    self.transceiver = transceiver
    self.node_id_str = node_id
    # Pad or truncate node_id to 8 bytes
    self.node_id = node_id.encode("utf-8")[:8].ljust(8, b"\x00")

    self.transceiver.set_channel(channel)
    self.transceiver.set_receive_callback(self._on_receive)

    self.seq_num = 0
    self.lock = threading.Lock()

    # routing_table[dest] = (next_hop, hops, expiry)
    self.routing_table: dict[bytes, tuple[bytes, int, float]] = {}

    # (orig_src, seq, f_type)
    self.seen_messages: set[tuple[bytes, int, int]] = set()

    # (dest, seq) -> event
    self.pending_acks: dict[tuple[bytes, int], threading.Event] = {}

    # dest -> event
    self.route_events: dict[bytes, threading.Event] = {}

    self.on_message_callback: Callable[[str, bytes], None] = None

  def set_message_callback(self, callback: Callable[[str, bytes], None]):
    """callback(sender_id_str, payload_bytes)"""
    self.on_message_callback = callback

  def start_discovery(self, interval_seconds: float = 5.0):
    """Starts broadcasting discovery beacons periodically."""

    def discover_loop():
      while True:
        with self.lock:
          seq = self.seq_num
          self.seq_num += 1
        self._transmit_raw_frame(
            FrameType.DISCOVERY,
            1,
            seq,
            self.node_id,
            self.BROADCAST_MAC,
            self.node_id,
            self.BROADCAST_MAC,
            b"",
        )
        time.sleep(interval_seconds)

    threading.Thread(target=discover_loop, daemon=True).start()

  def _get_route(self, dest: bytes, timeout: float = 2.0) -> bytes:
    with self.lock:
      if dest in self.routing_table:
        # check expiry
        if time.time() < self.routing_table[dest][2]:
          return self.routing_table[dest][0]
        else:
          del self.routing_table[dest]

      if dest not in self.route_events:
        self.route_events[dest] = threading.Event()
      event = self.route_events[dest]
      event.clear()

    # Send RREQ
    with self.lock:
      seq = self.seq_num
      self.seq_num += 1

    self._transmit_raw_frame(
        FrameType.RREQ,
        10,
        seq,
        self.node_id,
        self.BROADCAST_MAC,
        self.node_id,
        self.BROADCAST_MAC,
        dest,
    )

    if event.wait(timeout):
      with self.lock:
        if dest in self.routing_table:
          return self.routing_table[dest][0]
    return None

  def send_message(
      self, dest_id: str, data: bytes, timeout: float = 5.0, max_retries: int = 3
  ) -> bool:
    """Sends data to a specific destination over the mesh and waits for an ACK. Returns True if delivered."""
    dest_bytes = dest_id.encode("utf-8")[:8].ljust(8, b"\x00")

    for attempt in range(max_retries + 1):
      next_hop = self._get_route(dest_bytes, timeout=timeout / 2)
      if not next_hop:
        continue  # Retry route discovery

      with self.lock:
        seq = self.seq_num
        self.seq_num += 1

      ack_event = threading.Event()
      msg_id = (dest_bytes, seq)
      with self.lock:
        self.pending_acks[msg_id] = ack_event

      self._transmit_raw_frame(
          FrameType.DATA,
          10,
          seq,
          self.node_id,
          dest_bytes,
          self.node_id,
          next_hop,
          data,
      )

      if ack_event.wait(timeout):
        with self.lock:
          self.pending_acks.pop(msg_id, None)
        return True

      ack_event.clear()
      # If ACK times out, the route might be broken. Delete it to force a new RREQ.
      with self.lock:
        self.routing_table.pop(dest_bytes, None)
        self.pending_acks.pop(msg_id, None)

    return False

  def broadcast_message(self, data: bytes):
    """Sends data to all nodes."""
    with self.lock:
      seq = self.seq_num
      self.seq_num += 1
    self._transmit_raw_frame(
        FrameType.DATA,
        10,
        seq,
        self.node_id,
        self.BROADCAST_MAC,
        self.node_id,
        self.BROADCAST_MAC,
        data,
    )

  def _transmit_raw_frame(
      self,
      frame_type: int,
      ttl: int,
      seq: int,
      orig_src: bytes,
      final_dest: bytes,
      transmitter: bytes,
      next_hop: bytes,
      payload: bytes,
  ):
    frame_obj = MeshFrame(
        frame_type, ttl, seq, orig_src, final_dest, transmitter, next_hop, payload
    )
    packed_frame = frame_obj.pack()

    # CSMA/CA: Listen Before Talk
    start_time = time.time()
    attempts = 0
    while (
        time.time() - start_time < 3.0
    ):  # Wait up to 3 seconds for channel to clear
      if not self.transceiver.is_busy():
        time.sleep(random.uniform(0.001, 0.005))
        if not self.transceiver.is_busy():
          self.transceiver.broadcast(packed_frame)
          return

      wait_time = min(0.1, random.uniform(0.005, 0.01) * (2**attempts))
      time.sleep(wait_time)
      attempts += 1

  def _on_receive(self, data: bytes):
    frame = MeshFrame.unpack(data)
    if not frame:
      return

    # Ignore frames we originated (they bounced back through neighbors)
    if frame.orig_src == self.node_id:
      return

    # Ignore frames we transmitted (heard our own broadcast)
    if frame.transmitter == self.node_id:
      return

    # AODV Rule: Ignore unicast frames not meant for us (unless it's a broadcast)
    if frame.next_hop != self.node_id and frame.next_hop != self.BROADCAST_MAC:
      return

    # 2. Deduplicate
    msg_id = (frame.orig_src, frame.seq, frame.frame_type)
    with self.lock:
      is_duplicate = msg_id in self.seen_messages
      self.seen_messages.add(msg_id)

    # 3. Opportunistically learn route from transmitter
    with self.lock:
      if frame.transmitter != self.node_id:
        self.routing_table[frame.transmitter] = (
            frame.transmitter,
            1,
            time.time() + 300,
        )

    # 4. Process frame
    if frame.frame_type == FrameType.RREQ:
      if is_duplicate:
        return
      target_dest = frame.payload
      hops = 10 - frame.ttl + 1

      with self.lock:
        if (
            frame.orig_src not in self.routing_table
            or self.routing_table[frame.orig_src][1] > hops
        ):
          self.routing_table[frame.orig_src] = (
              frame.transmitter,
              hops,
              time.time() + 300,
          )

      if target_dest == self.node_id:
        # We are the target! Send RREP back.
        with self.lock:
          rrep_seq = self.seq_num
          self.seq_num += 1
          rrep_next_hop = self.routing_table[frame.orig_src][0]
        self._transmit_raw_frame(
            FrameType.RREP,
            10,
            rrep_seq,
            self.node_id,
            frame.orig_src,
            self.node_id,
            rrep_next_hop,
            self.node_id,
        )
      else:
        # Rebroadcast RREQ
        if frame.ttl > 1:
          self._transmit_raw_frame(
              FrameType.RREQ,
              frame.ttl - 1,
              frame.seq,
              frame.orig_src,
              frame.final_dest,
              self.node_id,
              self.BROADCAST_MAC,
              frame.payload,
          )

    elif frame.frame_type == FrameType.RREP:
      if is_duplicate:
        return
      target_dest = frame.payload
      hops = 10 - frame.ttl + 1

      with self.lock:
        if (
            target_dest not in self.routing_table
            or self.routing_table[target_dest][1] > hops
        ):
          self.routing_table[target_dest] = (
              frame.transmitter,
              hops,
              time.time() + 300,
          )

      if frame.final_dest == self.node_id:
        with self.lock:
          if target_dest in self.route_events:
            self.route_events[target_dest].set()
      else:
        # Forward RREP
        if frame.ttl > 1:
          with self.lock:
            if frame.final_dest in self.routing_table:
              fwd_next_hop = self.routing_table[frame.final_dest][0]
              self._transmit_raw_frame(
                  FrameType.RREP,
                  frame.ttl - 1,
                  frame.seq,
                  frame.orig_src,
                  frame.final_dest,
                  self.node_id,
                  fwd_next_hop,
                  frame.payload,
              )

    elif frame.frame_type == FrameType.DATA:
      if frame.final_dest == self.node_id:
        # We must ACK it
        with self.lock:
          ack_seq = self.seq_num
          self.seq_num += 1
          ack_next_hop = self.routing_table.get(frame.orig_src, (None,))[0]
        if ack_next_hop:
          import struct

          ack_payload = struct.pack("!I", frame.seq)
          self._transmit_raw_frame(
              FrameType.ACK,
              10,
              ack_seq,
              self.node_id,
              frame.orig_src,
              self.node_id,
              ack_next_hop,
              ack_payload,
          )

        if not is_duplicate:
          src_str = frame.orig_src.rstrip(b"\x00").decode(
              "utf-8", errors="ignore"
          )
          if self.on_message_callback:
            self.on_message_callback(src_str, frame.payload)
      elif frame.final_dest == self.BROADCAST_MAC:
        if not is_duplicate:
          src_str = frame.orig_src.rstrip(b"\x00").decode(
              "utf-8", errors="ignore"
          )
          if self.on_message_callback:
            self.on_message_callback(src_str, frame.payload)
          if frame.ttl > 1:
            self._transmit_raw_frame(
                FrameType.DATA,
                frame.ttl - 1,
                frame.seq,
                frame.orig_src,
                frame.final_dest,
                self.node_id,
                self.BROADCAST_MAC,
                frame.payload,
            )
      else:
        if is_duplicate:
          return
        # Forward unicast DATA
        if frame.ttl > 1:
          with self.lock:
            if frame.final_dest in self.routing_table:
              fwd_next_hop = self.routing_table[frame.final_dest][0]
              self._transmit_raw_frame(
                  FrameType.DATA,
                  frame.ttl - 1,
                  frame.seq,
                  frame.orig_src,
                  frame.final_dest,
                  self.node_id,
                  fwd_next_hop,
                  frame.payload,
              )

    elif frame.frame_type == FrameType.ACK:
      if is_duplicate:
        return
      if frame.final_dest == self.node_id:
        if len(frame.payload) == 4:
          import struct

          acked_seq = struct.unpack("!I", frame.payload)[0]
          ack_id = (frame.orig_src, acked_seq)
          with self.lock:
            if ack_id in self.pending_acks:
              self.pending_acks[ack_id].set()
      else:
        # Forward ACK
        if frame.ttl > 1:
          with self.lock:
            if frame.final_dest in self.routing_table:
              fwd_next_hop = self.routing_table[frame.final_dest][0]
              self._transmit_raw_frame(
                  FrameType.ACK,
                  frame.ttl - 1,
                  frame.seq,
                  frame.orig_src,
                  frame.final_dest,
                  self.node_id,
                  fwd_next_hop,
                  frame.payload,
              )
