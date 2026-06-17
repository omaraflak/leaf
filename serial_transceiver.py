import asyncio
import logging
import threading
import time
from typing import Callable, Awaitable, Optional
from transceiver import Transceiver
from frame import MeshFrame

try:
  import serial
except ImportError:
  serial = None

logger = logging.getLogger("leaf.serial")


class SerialTransceiver(Transceiver):
  """
  A Transceiver implementation that interfaces with a physical radio module
  via a serial COM port. Uses pyserial to read and write bytes.

  The blocking serial reads run in a background thread and bridge into
  the asyncio event loop via run_coroutine_threadsafe.
  """

  def __init__(self, port: str, baudrate: int = 9600):
    if serial is None:
      raise ImportError(
          "pyserial is not installed. Please install it using 'pip install pyserial'"
      )

    self.port = port
    self.baudrate = baudrate
    self.serial = serial.Serial(port, baudrate, timeout=0.1)
    self.callback: Optional[Callable[[bytes], Awaitable[None]]] = None
    self._running = True
    self._loop: Optional[asyncio.AbstractEventLoop] = None

  def set_receive_callback(self, callback: Callable[[bytes], Awaitable[None]]) -> None:
    self.callback = callback
    self._loop = asyncio.get_event_loop()
    self._receive_thread = threading.Thread(
        target=self._receive_loop, daemon=True)
    self._receive_thread.start()

  async def broadcast(self, data: bytes) -> None:
    """Writes raw frame data to the serial port (offloaded to executor)."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, self._blocking_write, data)

  def is_busy(self) -> bool:
    """
    Without a hardware Carrier Sense (CS) pin, we approximate 'busy'
    by checking if the serial buffer is currently receiving bytes.
    """
    if self.serial.is_open:
      return self.serial.in_waiting > 0
    return False

  def close(self):
    """Stops the read thread and closes the serial port."""
    self._running = False
    if self.serial.is_open:
      self.serial.close()

  def _blocking_write(self, data: bytes):
    if self.serial.is_open:
      self.serial.write(data)
      self.serial.flush()

  def _receive_loop(self):
    """
    Blocking read loop running in a background thread. Extracts complete
    MeshFrames from the serial stream and dispatches them to the async
    callback on the event loop.
    """
    buffer = bytearray()
    while self._running:
      try:
        if self.serial.is_open and self.serial.in_waiting > 0:
          data = self.serial.read(self.serial.in_waiting)
          buffer.extend(data)

          # Stream parsing loop
          while True:
            frame, consumed = MeshFrame.parse_from_buffer(buffer)
            if frame:
              frame_data = bytes(buffer[:consumed])
              buffer = buffer[consumed:]
              if self.callback and self._loop:
                asyncio.run_coroutine_threadsafe(
                    self.callback(frame_data), self._loop
                )
            else:
              if consumed > 0:
                buffer = buffer[consumed:]
              else:
                break
        else:
          time.sleep(0.01)
      except Exception as e:
        logger.error("Serial read error: %s", e)
        time.sleep(1.0)
