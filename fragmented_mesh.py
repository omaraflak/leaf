import asyncio
import logging
import struct
import time
from typing import Callable

from mesh_protocol import MeshProtocol
from transceiver import Transceiver

logger = logging.getLogger("leaf.fragmented_mesh")


class FragmentedMeshProtocol:
  """A protocol layer built on top of MeshProtocol that supports sending

  arbitrarily large payloads by fragmenting them into chunks, and reassembling
  them at the destination.
  """

  FRAGMENT_SIZE = 200
  INCOMPLETE_TIMEOUT_SEC = 30.0
  CLEANUP_INTERVAL_SEC = 10.0

  def __init__(self, transceiver: Transceiver, node_id: str, mobile: bool = False):
    self.mesh = MeshProtocol(transceiver, node_id, mobile)
    self.mesh.set_message_callback(self._on_mesh_message)

    self.on_message_callback: Callable[[str, bytes], None] | None = None

    # Message ID counter for outgoing messages (1 byte, wraps at 256)
    self._next_msg_id = 0

    # (sender_id, msg_id) -> {"chunks": {chunk_idx: data}, "total": total, "last_update": timestamp}
    self._incoming_messages: dict[tuple[str, int], dict] = {}

    # Track background tasks
    self._background_tasks: set[asyncio.Task] = set()

    # Start background cleanup task
    self._fire_and_forget(self._cleanup_incoming_messages_loop())

  def set_message_callback(self, callback: Callable[[str, bytes], None]):
    """callback(sender_id_str, payload_bytes)"""
    self.on_message_callback = callback

  async def send_message(
      self, dest_id: str, data: bytes, timeout: float = 5.0, max_retries: int = 3
  ) -> bool:
    """Sends arbitrarily large data to a destination by fragmenting it."""
    msg_id = self._next_msg_id
    self._next_msg_id = (self._next_msg_id + 1) % 256

    # Calculate chunks
    total_chunks = (len(data) + self.FRAGMENT_SIZE - 1) // self.FRAGMENT_SIZE
    if total_chunks == 0:
      total_chunks = 1  # Send at least one empty fragment if payload is empty

    for chunk_idx in range(total_chunks):
      start = chunk_idx * self.FRAGMENT_SIZE
      end = start + self.FRAGMENT_SIZE
      chunk_data = data[start:end]

      # Pack 9-byte header: msg_id (1), chunk_idx (4), total_chunks (4)
      header = struct.pack("!B I I", msg_id, chunk_idx, total_chunks)
      chunk_payload = header + chunk_data

      # Send chunk. Since it is unicast, MeshProtocol guarantees delivery per chunk (with ACKs).
      success = await self.mesh.send_message(
          dest_id, chunk_payload, timeout, max_retries
      )
      if not success:
        logger.error(
            "Failed to send chunk %d/%d for message %d to %s",
            chunk_idx + 1,
            total_chunks,
            msg_id,
            dest_id,
        )
        return False

    return True

  async def broadcast_message(self, data: bytes):
    """Broadcasts arbitrarily large data by fragmenting it."""
    msg_id = self._next_msg_id
    self._next_msg_id = (self._next_msg_id + 1) % 256

    total_chunks = (len(data) + self.FRAGMENT_SIZE - 1) // self.FRAGMENT_SIZE
    if total_chunks == 0:
      total_chunks = 1

    for chunk_idx in range(total_chunks):
      start = chunk_idx * self.FRAGMENT_SIZE
      end = start + self.FRAGMENT_SIZE
      chunk_data = data[start:end]

      header = struct.pack("!B I I", msg_id, chunk_idx, total_chunks)
      chunk_payload = header + chunk_data

      # Broadcast chunk (no end-to-end ACKs)
      await self.mesh.broadcast_message(chunk_payload)

  def close(self):
    """Closes the protocol, cancelling background tasks and closing underlying mesh."""
    self.mesh.close()
    for task in list(self._background_tasks):
      task.cancel()

  def _fire_and_forget(self, coro):
    task = asyncio.create_task(coro)
    self._background_tasks.add(task)
    task.add_done_callback(self._background_tasks.discard)

  def _on_mesh_message(self, sender_id: str, payload: bytes):
    if len(payload) < 9:
      logger.warning(
          "Received invalid fragment from %s: payload too short", sender_id
      )
      return

    msg_id, chunk_idx, total_chunks = struct.unpack("!B I I", payload[:9])
    chunk_data = payload[9:]

    key = (sender_id, msg_id)
    now = time.time()

    if key not in self._incoming_messages:
      self._incoming_messages[key] = {
          "chunks": {},
          "total": total_chunks,
          "last_update": now,
      }

    msg_state = self._incoming_messages[key]
    msg_state["chunks"][chunk_idx] = chunk_data
    msg_state["last_update"] = now

    if len(msg_state["chunks"]) == msg_state["total"]:
      # All chunks received! Reassemble.
      self._incoming_messages.pop(key)
      assembled_data = b"".join(
          msg_state["chunks"][idx] for idx in sorted(msg_state["chunks"].keys())
      )
      logger.info(
          "Successfully reassembled message %d from %s (%d bytes)",
          msg_id,
          sender_id,
          len(assembled_data),
      )
      if self.on_message_callback:
        self.on_message_callback(sender_id, assembled_data)

  async def _cleanup_incoming_messages_loop(self):
    try:
      while True:
        await asyncio.sleep(self.CLEANUP_INTERVAL_SEC)
        self._cleanup_incoming_messages()
    except asyncio.CancelledError:
      logger.debug("Incomplete messages cleanup task cancelled")

  def _cleanup_incoming_messages(self):
    now = time.time()
    expired = [
        key
        for key, state in self._incoming_messages.items()
        if now - state["last_update"] >= self.INCOMPLETE_TIMEOUT_SEC
    ]
    for key in expired:
      logger.warning(
          "Timeout receiving message %d from %s. Dropped %d/%d chunks.",
          key[1],
          key[0],
          len(self._incoming_messages[key]["chunks"]),
          self._incoming_messages[key]["total"],
      )
      self._incoming_messages.pop(key, None)
