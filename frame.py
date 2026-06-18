import struct
import binascii
from typing import Optional


class FrameType:
  DATA = 1
  ACK = 2
  RREQ = 3
  RREP = 4


class FrameFlags:
  STATIONARY = 0x00
  MOBILE = 0x01


class MeshFrame:
  """
Utility class to encapsulate building, parsing, and verifying
radio packets for the AODV mesh protocol.
"""

  MAGIC = b"\xaa\xbb"
  BROADCAST_MAC = b"\xff\xff\xff\xff\xff\xff\xff\xff"
  # Format: MAGIC (2), Type (1), TTL (1), Seq (4), OrigSrc (8), FinalDest (8),
  #         Transmitter (8), NextHop (8), Flags (1), PayloadLen (2)
  HEADER_FMT = "!2s B B I 8s 8s 8s 8s B H"
  HEADER_SIZE = struct.calcsize(HEADER_FMT)

  def __init__(
      self,
      frame_type: int,
      ttl: int,
      seq: int,
      orig_src: bytes,
      final_dest: bytes,
      transmitter: bytes,
      next_hop: bytes,
      payload: bytes,
      flags: int,
  ):
    self.frame_type = frame_type
    self.ttl = ttl
    self.seq = seq
    self.orig_src = orig_src
    self.final_dest = final_dest
    self.transmitter = transmitter
    self.next_hop = next_hop
    self.payload = payload
    self.flags = flags

  def pack(self) -> bytes:
    payload_len = len(self.payload)
    header = struct.pack(
        self.HEADER_FMT,
        self.MAGIC,
        self.frame_type,
        self.ttl,
        self.seq,
        self.orig_src,
        self.final_dest,
        self.transmitter,
        self.next_hop,
        self.flags,
        payload_len,
    )
    frame_without_crc = header + self.payload
    crc = binascii.crc32(frame_without_crc) & 0xFFFFFFFF
    return frame_without_crc + struct.pack("!I", crc)

  @classmethod
  def unpack(cls, data: bytes):
    """Returns a MeshFrame if valid, otherwise None."""
    if len(data) < cls.HEADER_SIZE + 4:
      return None

    if not data.startswith(cls.MAGIC):
      return None

    try:
      unpacked = struct.unpack(cls.HEADER_FMT, data[: cls.HEADER_SIZE])
      (
          magic,
          f_type,
          ttl,
          seq,
          orig_src,
          final_dest,
          transmitter,
          next_hop,
          flags,
          payload_len,
      ) = unpacked
    except struct.error:
      return None

    if len(data) < cls.HEADER_SIZE + payload_len + 4:
      return None

    payload = data[cls.HEADER_SIZE: cls.HEADER_SIZE + payload_len]
    received_crc = struct.unpack(
        "!I",
        data[cls.HEADER_SIZE + payload_len: cls.HEADER_SIZE + payload_len + 4],
    )[0]

    frame_without_crc = data[: cls.HEADER_SIZE + payload_len]
    calculated_crc = binascii.crc32(frame_without_crc) & 0xFFFFFFFF

    if received_crc != calculated_crc:
      return None  # Checksum failed

    return cls(
        f_type, ttl, seq, orig_src, final_dest, transmitter, next_hop,
        payload, flags
    )

  @classmethod
  def parse_from_buffer(cls, buffer: bytearray) -> tuple[Optional["MeshFrame"], int]:
    """
Parses a MeshFrame from a streaming bytearray buffer.
Returns:
  (frame, bytes_consumed):
    - If a frame is successfully parsed, frame is MeshFrame, bytes_consumed is total_frame_size.
    - If a frame is incomplete, frame is None, bytes_consumed is 0.
    - If the buffer starts with invalid bytes (no magic), frame is None, bytes_consumed is number of skipped bytes.
"""
    if not buffer:
      return None, 0

    magic_idx = buffer.find(cls.MAGIC)
    if magic_idx == -1:
      # No magic found, discard the entire buffer
      return None, len(buffer)

    if magic_idx > 0:
      # Discard everything before the magic bytes
      return None, magic_idx

    if len(buffer) < cls.HEADER_SIZE:
      return None, 0

    try:
      header_data = struct.unpack(cls.HEADER_FMT, buffer[: cls.HEADER_SIZE])
      payload_len = header_data[9]
      total_frame_size = cls.HEADER_SIZE + payload_len + 4

      if len(buffer) < total_frame_size:
        # Incomplete frame
        return None, 0

      frame_data = bytes(buffer[:total_frame_size])
      frame = cls.unpack(frame_data)
      if frame is None:
        # Unpack failed (e.g. CRC mismatch), discard the first byte of magic to try again
        return None, 1

      return frame, total_frame_size
    except struct.error:
      # Header formatting error, discard first byte to recover
      return None, 1
