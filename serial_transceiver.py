import threading
import time
import struct
from typing import Callable, Optional
from transceiver import Transceiver
from frame import MeshFrame

try:
  import serial
except ImportError:
  # Fallback if pyserial is not installed
  serial = None


class SerialTransceiver(Transceiver):
  """
  A Transceiver implementation that interfaces with a physical radio module
  via a serial COM port. Uses pyserial to read and write bytes.
  """

  def __init__(self, port: str, baudrate: int = 9600):
    if serial is None:
      raise ImportError(
          "pyserial is not installed. Please install it using 'pip install pyserial'"
      )

    self.port = port
    self.baudrate = baudrate
    self.serial = serial.Serial(port, baudrate, timeout=0.1)
    self.callback: Optional[Callable[[bytes], None]] = None
    self.channel = 0
    self._running = True

    self.receive_thread = threading.Thread(
        target=self._receive_loop, daemon=True)
    self.receive_thread.start()

  def set_channel(self, channel: int) -> None:
    self.channel = channel
    # Note: Hardware-specific AT commands might be required here to actually change the radio channel.
    # Example: self.serial.write(f"AT+CHAN={channel}\r\n".encode())

  def broadcast(self, data: bytes) -> None:
    """Writes raw frame data to the serial port."""
    if self.serial.is_open:
      self.serial.write(data)
      self.serial.flush()

  def set_receive_callback(self, callback: Callable[[bytes], None]) -> None:
    self.callback = callback

  def is_busy(self) -> bool:
    """
    Without a hardware Carrier Sense (CS) pin, we approximate 'busy'
    by checking if the serial buffer is currently receiving bytes.
    """
    if self.serial.is_open:
      return self.serial.in_waiting > 0
    return False

  def _receive_loop(self):
    """
    Continuously reads from the serial port. Since serial is a stream of bytes,
    this loops buffers the data and extracts complete MeshFrames based on MAGIC bytes.
    """
    buffer = bytearray()
    while self._running:
      try:
        if self.serial.is_open and self.serial.in_waiting > 0:
          data = self.serial.read(self.serial.in_waiting)
          buffer.extend(data)

          # Stream parsing loop
          while True:
            # 1. Look for MAGIC bytes sync marker
            magic_idx = buffer.find(MeshFrame.MAGIC)
            if magic_idx == -1:
              buffer.clear()
              break

            # 2. Discard any noise before MAGIC
            if magic_idx > 0:
              buffer = buffer[magic_idx:]

            # 3. Wait until we have at least the full header
            if len(buffer) < MeshFrame.HEADER_SIZE:
              break

            try:
              # 4. Unpack header to read the expected payload length
              header_data = struct.unpack(
                  MeshFrame.HEADER_FMT, buffer[: MeshFrame.HEADER_SIZE]
              )
              payload_len = header_data[8]
              total_frame_size = (
                  MeshFrame.HEADER_SIZE + payload_len + 4
              )  # 4 for CRC32

              # 5. Wait until the entire frame has arrived
              if len(buffer) < total_frame_size:
                break

              # 6. Extract the complete frame and advance the buffer
              frame_data = bytes(buffer[:total_frame_size])
              buffer = buffer[total_frame_size:]

              if self.callback:
                self.callback(frame_data)

            except struct.error:
              # Malformed header, pop 1 byte to keep searching for a valid MAGIC
              buffer = buffer[1:]
        else:
          time.sleep(0.01)
      except Exception as e:
        print(f"Serial read error: {e}")
        time.sleep(1.0)

  def close(self):
    """Stops the read thread and closes the serial port."""
    self._running = False
    if self.serial.is_open:
      self.serial.close()
