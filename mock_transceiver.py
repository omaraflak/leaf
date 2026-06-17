import asyncio
import random
from typing import Callable, Awaitable
from transceiver import Transceiver


class MockMedium:
  """
  Simulates the physical air/medium for radio communications.
  Handles device positions, distance limits, delays, and collisions.
  """

  def __init__(self, max_range_m: float = 3000.0, bytes_per_sec: float = 1000.0):
    self.transceivers: list["MockTransceiver"] = []
    self.max_range_m = max_range_m
    self.bytes_per_sec = bytes_per_sec

  def register(self, transceiver: "MockTransceiver"):
    self.transceivers.append(transceiver)

  async def transmit(self, sender: "MockTransceiver", data: bytes):
    """Simulates transmission of data. Calculates duration based on mock baud rate."""
    duration = len(data) / self.bytes_per_sec

    receivers_in_range = [
        t
        for t in self.transceivers
        if t != sender and self._distance(sender, t) <= self.max_range_m
    ]

    # Notify receivers that a signal is starting (synchronous — just sets state)
    for r in receivers_in_range:
      r._air_signal_start(data)

    async def finish():
      await asyncio.sleep(duration)
      for r in receivers_in_range:
        await r._air_signal_end(data)

    asyncio.create_task(finish())

  def _distance(self, t1: "MockTransceiver", t2: "MockTransceiver") -> float:
    return ((t1.x - t2.x) ** 2 + (t1.y - t2.y) ** 2) ** 0.5


class MockTransceiver(Transceiver):
  """
  A mock implementation of the Transceiver interface that uses MockMedium.
  """

  def __init__(self, medium: MockMedium, x: float, y: float, name: str = ""):
    self.medium = medium
    self.x = x
    self.y = y
    self.name = name
    self.callback: Callable[[bytes], Awaitable[None]] | None = None

    # State for receiving: id(data) -> {"data": data, "collided": bool}
    self.active_signals: dict[int, dict] = {}

    self.medium.register(self)

  async def broadcast(self, data: bytes) -> None:
    await self.medium.transmit(self, data)

  def set_receive_callback(self, callback: Callable[[bytes], Awaitable[None]]) -> None:
    self.callback = callback

  def is_busy(self) -> bool:
    """Returns True if the antenna is currently picking up any signals."""
    return len(self.active_signals) > 0

  def _air_signal_start(self, data: bytes):
    """Called by MockMedium when a signal reaches this transceiver's antenna."""
    sig_id = id(data)
    self.active_signals[sig_id] = {"data": data, "collided": False}
    # If there are multiple active signals, they interfere and all collide
    if len(self.active_signals) > 1:
      for sig in self.active_signals.values():
        sig["collided"] = True

  async def _air_signal_end(self, data: bytes):
    """Called by MockMedium when a signal finishes transmitting."""
    sig_id = id(data)
    if sig_id not in self.active_signals:
      return

    sig_info = self.active_signals.pop(sig_id)

    if not sig_info["collided"]:
      # Successfully received cleanly without overlap
      if self.callback:
        received_data = sig_info["data"]

        async def delayed_callback():
          await asyncio.sleep(0.005)  # Simulated processing delay
          await self.callback(received_data)

        asyncio.create_task(delayed_callback())
    else:
      # Signal was mangled by a collision. Pass garbage to simulate noise.
      garbage = bytes(random.randint(0, 255) for _ in range(len(data)))
      if self.callback:

        async def delayed_callback_garbage():
          await asyncio.sleep(0.005)
          await self.callback(garbage)

        asyncio.create_task(delayed_callback_garbage())
