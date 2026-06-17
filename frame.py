import struct
import binascii


class FrameType:
  DISCOVERY = 1
  DATA = 2
  ACK = 3
  RREQ = 4
  RREP = 5


class MeshFrame:
  """
  Utility class to encapsulate building, parsing, and verifying
  radio packets for the AODV mesh protocol.
  """

  MAGIC = b"\xaa\xbb"
  # Format: MAGIC (2), Type (1), TTL (1), Seq (4), OrigSrc (8), FinalDest (8), Transmitter (8), NextHop (8), PayloadLen (2)
  HEADER_FMT = "!2s B B I 8s 8s 8s 8s H"
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
  ):
    self.frame_type = frame_type
    self.ttl = ttl
    self.seq = seq
    self.orig_src = orig_src
    self.final_dest = final_dest
    self.transmitter = transmitter
    self.next_hop = next_hop
    self.payload = payload

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
        f_type, ttl, seq, orig_src, final_dest, transmitter, next_hop, payload
    )
