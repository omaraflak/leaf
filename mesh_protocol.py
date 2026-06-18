import asyncio
import logging
import random
import struct
import time
from typing import Callable
from frame import FrameType, FrameFlags, MeshFrame
from transceiver import Transceiver

logger = logging.getLogger("leaf.mesh")


class MeshProtocol:
  """
A mesh networking protocol built on top of a raw radio Transceiver.
Implements AODV-like routing, CSMA/CA, End-to-End ACKs, and
mobility-aware route management.
"""

  MAX_TTL = 10
  ROUTE_EXPIRY_STATIONARY = 1800.0  # 30 minutes
  ROUTE_EXPIRY_MOBILE = 60.0  # 1 minute
  ROUTE_CLEANUP_INTERVAL = 10.0
  MOBILITY_HOP_PENALTY = 2
  BROADCAST_JITTER_S = 0.05

  def __init__(self, transceiver: Transceiver, node_id: str, mobile: bool = False):
    self.transceiver = transceiver
    self.mobile = mobile

    # Pad or truncate node_id to 8 bytes
    self.node_id = node_id.encode("utf-8")[:8].ljust(8, b"\x00")

    self.transceiver.set_receive_callback(self._on_receive)

    self.seq_num = 0

    # routing_table[dest] = (next_hop, hops, expiry, next_hop_mobile)
    self.routing_table: dict[bytes, tuple[bytes, int, float, bool]] = {}

    # (orig_src, seq, f_type) -> timestamp
    self.seen_messages: dict[tuple[bytes, int, int], float] = {}

    # (dest, seq) -> event
    self.pending_acks: dict[tuple[bytes, int], asyncio.Event] = {}

    # dest -> event (active route discoveries)
    self._active_discoveries: dict[bytes, asyncio.Event] = {}

    self.on_message_callback: Callable[[str, bytes], None] | None = None

    # Track background tasks to prevent garbage collection
    self._background_tasks: set[asyncio.Task] = set()

    self._tx_lock = asyncio.Lock()

    logger.info("Initialized MeshProtocol node: %s (mobile=%s)",
                node_id, mobile)

    # Start periodic cleanup of routing table
    self._fire_and_forget(self._cleanup_routing_table_loop())

  def set_message_callback(self, callback: Callable[[str, bytes], None]):
    """callback(sender_id_str, payload_bytes)"""
    self.on_message_callback = callback

  async def send_message(
      self, dest_id: str, data: bytes, timeout: float = 5.0, max_retries: int = 3
  ) -> bool:
    """Sends data to a specific destination over the mesh and waits for an ACK. Returns True if delivered."""
    dest_bytes = dest_id.encode("utf-8")[:8].ljust(8, b"\x00")
    logger.info("Sending message to %s (%s)", dest_id, dest_bytes)

    for attempt in range(max_retries + 1):
      logger.debug("Route discovery attempt %d to %s", attempt, dest_id)
      next_hop = await self._get_route(dest_bytes, timeout / 2)
      if not next_hop:
        logger.warning(
            "Could not find route to %s on attempt %d", dest_id, attempt
        )
        continue

      seq = self.seq_num
      self.seq_num += 1

      ack_event = asyncio.Event()
      msg_id = (dest_bytes, seq)
      self.pending_acks[msg_id] = ack_event

      logger.debug(
          "Transmitting DATA packet seq %d to %s via next hop %s",
          seq,
          dest_id,
          next_hop,
      )
      await self._transmit_raw_frame(
          FrameType.DATA,
          self.MAX_TTL,
          seq,
          self.node_id,
          dest_bytes,
          self.node_id,
          next_hop,
          data,
      )

      try:
        await asyncio.wait_for(ack_event.wait(), timeout)
        self.pending_acks.pop(msg_id, None)
        logger.info(
            "Successfully delivered message to %s (ack seq %d)", dest_id, seq
        )
        return True
      except asyncio.TimeoutError:
        logger.warning(
            "Timeout waiting for ACK for msg seq %d to %s. "
            "Clearing cached route.",
            seq,
            dest_id,
        )
        # If ACK times out, the route might be broken.
        # Delete it to force a new RREQ.
        self.routing_table.pop(dest_bytes, None)
        self.pending_acks.pop(msg_id, None)

    logger.error(
        "Failed to deliver message to %s after %d attempts", dest_id, max_retries
    )
    return False

  async def broadcast_message(self, data: bytes):
    """Sends data to all nodes."""
    seq = self.seq_num
    self.seq_num += 1
    logger.info("Broadcasting message, seq %d", seq)
    await self._transmit_raw_frame(
        FrameType.DATA,
        self.MAX_TTL,
        seq,
        self.node_id,
        MeshFrame.BROADCAST_MAC,
        self.node_id,
        MeshFrame.BROADCAST_MAC,
        data,
    )

  def close(self):
    """Closes the protocol, cancelling any background tasks."""
    logger.info("Closing MeshProtocol node: %s", self.node_id)
    for task in list(self._background_tasks):
      task.cancel()

  def _fire_and_forget(self, coro):
    """Schedule a coroutine as a background task."""
    task = asyncio.create_task(coro)
    self._background_tasks.add(task)
    task.add_done_callback(self._background_tasks.discard)

  async def _get_route(self, dest: bytes, timeout: float) -> bytes | None:
    if dest in self.routing_table:
      if time.time() < self.routing_table[dest][2]:
        logger.debug(
            "Route cache hit for %s: next hop %s",
            dest,
            self.routing_table[dest][0],
        )
        return self.routing_table[dest][0]
      else:
        logger.debug("Expired route cache for %s", dest)
        del self.routing_table[dest]

    if dest in self._active_discoveries:
      logger.debug(
          "Route discovery already in progress for %s. Waiting...", dest)
      event = self._active_discoveries[dest]
      try:
        await asyncio.wait_for(event.wait(), timeout)
      except asyncio.TimeoutError:
        pass
      if dest in self.routing_table:
        return self.routing_table[dest][0]
      return None

    # Start new route discovery
    logger.debug("Starting route discovery for %s", dest)
    event = asyncio.Event()
    self._active_discoveries[dest] = event

    # Send RREQ
    seq = self.seq_num
    self.seq_num += 1

    await self._transmit_raw_frame(
        FrameType.RREQ,
        self.MAX_TTL,
        seq,
        self.node_id,
        MeshFrame.BROADCAST_MAC,
        self.node_id,
        MeshFrame.BROADCAST_MAC,
        dest,
    )

    try:
      await asyncio.wait_for(event.wait(), timeout)
    except asyncio.TimeoutError:
      logger.debug("Route discovery timed out for %s", dest)
    finally:
      self._active_discoveries.pop(dest, None)

    if dest in self.routing_table:
      return self.routing_table[dest][0]
    return None

  async def _transmit_raw_frame(
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
        frame_type,
        ttl,
        seq,
        orig_src,
        final_dest,
        transmitter,
        next_hop,
        payload,
        flags=self._tx_flags(),
    )
    packed_frame = frame_obj.pack()

    if next_hop == MeshFrame.BROADCAST_MAC:
      await asyncio.sleep(random.uniform(0.0, self.BROADCAST_JITTER_S))

    async with self._tx_lock:

      # CSMA/CA: Listen Before Talk
      start_time = time.time()
      attempts = 0
      while time.time() - start_time < 3.0:
        if not self.transceiver.is_busy():
          await asyncio.sleep(random.uniform(0.001, 0.005))
          if not self.transceiver.is_busy():
            logger.debug(
                "Transmitting frame: type %d, seq %d", frame_type, seq
            )
            await self.transceiver.broadcast(packed_frame)
            return

        wait_time = min(0.1, random.uniform(0.005, 0.01) * (2**attempts))
        logger.debug("Channel busy. Backing off for %fs", wait_time)
        await asyncio.sleep(wait_time)
        attempts += 1

  def _update_route(
      self, dest: bytes, next_hop: bytes, hops: int, next_hop_mobile: bool
  ):
    """Updates the routing table if the new route is better."""
    route_expiry = (
        self.ROUTE_EXPIRY_MOBILE
        if next_hop_mobile
        else self.ROUTE_EXPIRY_STATIONARY
    )
    expiry = time.time() + route_expiry
    if dest not in self.routing_table:
      self.routing_table[dest] = (next_hop, hops, expiry, next_hop_mobile)
      return
    _, old_hops, _, old_mobile = self.routing_table[dest]
    if self._is_better_route(hops, next_hop_mobile, old_hops, old_mobile):
      self.routing_table[dest] = (next_hop, hops, expiry, next_hop_mobile)

  def _is_better_route(
      self, new_hops: int, new_mobile: bool, old_hops: int, old_mobile: bool
  ) -> bool:
    """Determines if a new route is better than an existing one.
Prefers stationary next-hops over mobile ones, tolerating up to
MOBILITY_HOP_PENALTY extra hops for stability."""
    if new_mobile == old_mobile:
      # Same mobility class: prefer fewer hops
      return new_hops < old_hops
    if not new_mobile and old_mobile:
      # New is stationary, old is mobile: accept up to N extra hops
      return new_hops <= old_hops + self.MOBILITY_HOP_PENALTY
    # New is mobile, old is stationary: only accept if significantly shorter
    return new_hops + self.MOBILITY_HOP_PENALTY < old_hops

  def _tx_flags(self) -> int:
    """Returns the flags byte for outgoing frames."""
    return FrameFlags.MOBILE if self.mobile else FrameFlags.STATIONARY

  def _cleanup_seen_messages(self):
    now = time.time()
    expiry_cutoff = now - 300.0
    self.seen_messages = {
        msg_id: timestamp
        for msg_id, timestamp in self.seen_messages.items()
        if timestamp > expiry_cutoff
    }

  async def _on_receive(self, data: bytes):
    frame = MeshFrame.unpack(data)
    if not frame:
      logger.warning("Received invalid/mangled frame (unpack failed)")
      return

    # Ignore frames we originated (they bounced back through neighbors)
    if frame.orig_src == self.node_id:
      return

    # Ignore frames we transmitted (heard our own broadcast)
    if frame.transmitter == self.node_id:
      return

    # AODV Rule: Ignore unicast frames not meant for us (unless broadcast)
    if frame.next_hop != self.node_id and frame.next_hop != MeshFrame.BROADCAST_MAC:
      return

    # Deduplicate
    msg_id = (frame.orig_src, frame.seq, frame.frame_type)
    is_duplicate = msg_id in self.seen_messages
    self.seen_messages[msg_id] = time.time()
    if len(self.seen_messages) > 1000:
      self._cleanup_seen_messages()

    # Extract transmitter mobility from flags
    tx_mobile = bool(frame.flags & FrameFlags.MOBILE)

    # Opportunistically learn route from transmitter
    if frame.transmitter != self.node_id:
      self._update_route(frame.transmitter, frame.transmitter, 1, tx_mobile)

    logger.debug(
        "Received frame type %d from %s (transmitter %s, mobile=%s) "
        "to %s. Duplicate=%s",
        frame.frame_type,
        frame.orig_src,
        frame.transmitter,
        tx_mobile,
        frame.final_dest,
        is_duplicate,
    )

    # Process frame
    if frame.frame_type == FrameType.RREQ:
      self._handle_rreq(frame, is_duplicate, tx_mobile)

    elif frame.frame_type == FrameType.RREP:
      self._handle_rrep(frame, is_duplicate, tx_mobile)

    elif frame.frame_type == FrameType.DATA:
      self._handle_data(frame, is_duplicate)

    elif frame.frame_type == FrameType.ACK:
      self._handle_ack(frame, is_duplicate)

  def _handle_rreq(self, frame: MeshFrame, is_duplicate: bool, tx_mobile: bool):
    if is_duplicate:
      return
    target_dest = frame.payload
    hops = self.MAX_TTL - frame.ttl + 1

    self._update_route(frame.orig_src, frame.transmitter, hops, tx_mobile)

    if target_dest == self.node_id:
      # We are the target! Send RREP back.
      rrep_seq = self.seq_num
      self.seq_num += 1
      rrep_next_hop = self.routing_table[frame.orig_src][0]
      logger.info(
          "We are target of RREQ from %s. Sending RREP via next hop %s",
          frame.orig_src,
          rrep_next_hop,
      )
      self._fire_and_forget(
          self._transmit_raw_frame(
              FrameType.RREP,
              self.MAX_TTL,
              rrep_seq,
              self.node_id,
              frame.orig_src,
              self.node_id,
              rrep_next_hop,
              self.node_id,
          )
      )
    else:
      # Rebroadcast RREQ
      if frame.ttl > 1:
        logger.debug(
            "Rebroadcasting RREQ for %s (ttl %d)", target_dest, frame.ttl - 1
        )
        self._fire_and_forget(
            self._transmit_raw_frame(
                FrameType.RREQ,
                frame.ttl - 1,
                frame.seq,
                frame.orig_src,
                frame.final_dest,
                self.node_id,
                MeshFrame.BROADCAST_MAC,
                frame.payload,
            )
        )

  def _handle_rrep(self, frame: MeshFrame, is_duplicate: bool, tx_mobile: bool):
    if is_duplicate:
      return
    target_dest = frame.payload
    hops = self.MAX_TTL - frame.ttl + 1

    self._update_route(target_dest, frame.transmitter, hops, tx_mobile)

    if frame.final_dest == self.node_id:
      logger.info("Received RREP establishing route to %s", target_dest)
      if target_dest in self._active_discoveries:
        self._active_discoveries[target_dest].set()
    else:
      # Forward RREP
      if frame.ttl > 1:
        if frame.final_dest in self.routing_table:
          fwd_next_hop = self.routing_table[frame.final_dest][0]
          logger.debug(
              "Forwarding RREP for %s to %s", target_dest, fwd_next_hop
          )
          self._fire_and_forget(
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
          )

  def _handle_data(self, frame: MeshFrame, is_duplicate: bool):
    if frame.final_dest == self.node_id:
      # We must ACK it
      ack_next_hop = self.routing_table.get(frame.orig_src, (None,))[0]
      if ack_next_hop:
        ack_seq = self.seq_num
        self.seq_num += 1
        ack_payload = struct.pack("!I", frame.seq)
        logger.debug(
            "Sending DATA ACK to %s via next hop %s",
            frame.orig_src,
            ack_next_hop,
        )
        self._fire_and_forget(
            self._transmit_raw_frame(
                FrameType.ACK,
                self.MAX_TTL,
                ack_seq,
                self.node_id,
                frame.orig_src,
                self.node_id,
                ack_next_hop,
                ack_payload,
            )
        )
      else:
        logger.warning(
            "Cannot send ACK back to %s: no route in table.", frame.orig_src
        )

      if not is_duplicate:
        src_str = frame.orig_src.rstrip(b"\x00").decode(
            "utf-8", errors="ignore"
        )
        logger.info("Received final DATA payload from %s", src_str)
        if self.on_message_callback:
          self.on_message_callback(src_str, frame.payload)

    elif frame.final_dest == MeshFrame.BROADCAST_MAC:
      if not is_duplicate:
        src_str = frame.orig_src.rstrip(b"\x00").decode(
            "utf-8", errors="ignore"
        )
        logger.info("Received broadcast DATA payload from %s", src_str)
        if self.on_message_callback:
          self.on_message_callback(src_str, frame.payload)
        if frame.ttl > 1:
          logger.debug(
              "Rebroadcasting broadcast DATA payload from %s", src_str
          )
          self._fire_and_forget(
              self._transmit_raw_frame(
                  FrameType.DATA,
                  frame.ttl - 1,
                  frame.seq,
                  frame.orig_src,
                  frame.final_dest,
                  self.node_id,
                  MeshFrame.BROADCAST_MAC,
                  frame.payload,
              )
          )
    else:
      if is_duplicate:
        return
      # Forward unicast DATA
      if frame.ttl > 1:
        if frame.final_dest in self.routing_table:
          fwd_next_hop = self.routing_table[frame.final_dest][0]
          logger.debug(
              "Forwarding DATA from %s to %s via %s",
              frame.orig_src,
              frame.final_dest,
              fwd_next_hop,
          )
          self._fire_and_forget(
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
          )
        else:
          logger.warning(
              "Dropped data packet: route to %s is missing", frame.final_dest
          )

  def _handle_ack(self, frame: MeshFrame, is_duplicate: bool):
    if is_duplicate:
      return
    if frame.final_dest == self.node_id:
      if len(frame.payload) == 4:
        acked_seq = struct.unpack("!I", frame.payload)[0]
        ack_id = (frame.orig_src, acked_seq)
        logger.debug(
            "Received ACK from %s for sequence %d", frame.orig_src, acked_seq
        )
        if ack_id in self.pending_acks:
          self.pending_acks[ack_id].set()
    else:
      # Forward ACK
      if frame.ttl > 1:
        if frame.final_dest in self.routing_table:
          fwd_next_hop = self.routing_table[frame.final_dest][0]
          logger.debug(
              "Forwarding ACK for %s to next hop %s",
              frame.final_dest,
              fwd_next_hop,
          )
          self._fire_and_forget(
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
          )

  async def _cleanup_routing_table_loop(self):
    try:
      while True:
        await asyncio.sleep(self.ROUTE_CLEANUP_INTERVAL)
        self._cleanup_routing_table()
    except asyncio.CancelledError:
      logger.debug("Routing table cleanup task cancelled")

  def _cleanup_routing_table(self):
    now = time.time()
    expired = [
        dest
        for dest, (_, _, expiry, _) in self.routing_table.items()
        if now >= expiry
    ]
    for dest in expired:
      logger.debug("Pruning expired route to %s", dest)
      self.routing_table.pop(dest, None)
