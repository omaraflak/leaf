import threading
import time
import random
from typing import Callable, List, Dict
from transceiver import Transceiver


class MockMedium:
  """
  Simulates the physical air/medium for radio communications.
  Handles device positions, distance limits, delays, and collisions.
  """

  def __init__(self, max_range_m: float = 3000.0, bytes_per_sec: float = 1000.0):
    self.transceivers = []
    self.max_range_m = max_range_m
    self.bytes_per_sec = bytes_per_sec
    self.lock = threading.Lock()

  def register(self, transceiver: "MockTransceiver"):
    with self.lock:
      self.transceivers.append(transceiver)

  def _distance(self, t1: "MockTransceiver", t2: "MockTransceiver") -> float:
    return ((t1.x - t2.x) ** 2 + (t1.y - t2.y) ** 2) ** 0.5

  def transmit(self, sender: "MockTransceiver", data: bytes):
    """Simulates transmission of data. Calculates duration based on mock baud rate."""
    duration = len(data) / self.bytes_per_sec

    with self.lock:
      receivers_in_range = [
          t
          for t in self.transceivers
          if t != sender and self._distance(sender, t) <= self.max_range_m
      ]

    # Notify receivers that a signal is starting
    for r in receivers_in_range:
      r._air_signal_start(data)

    def finish():
      time.sleep(duration)
      # Notify receivers that the signal has ended
      for r in receivers_in_range:
        r._air_signal_end(data)

    threading.Thread(target=finish, daemon=True).start()


class MockTransceiver(Transceiver):
  """
  A mock implementation of the Transceiver interface that uses MockMedium.
  """

  def __init__(self, medium: MockMedium, x: float, y: float, name: str = ""):
    self.medium = medium
    self.x = x
    self.y = y
    self.name = name
    self.callback = None
    self.lock = threading.Lock()

    # State for receiving: id(data) -> {"data": data, "collided": bool}
    self.active_signals = {}

    self.medium.register(self)

  def broadcast(self, data: bytes) -> None:
    self.medium.transmit(self, data)

  def set_receive_callback(self, callback: Callable[[bytes], None]) -> None:
    self.callback = callback

  def is_busy(self) -> bool:
    """Returns True if the antenna is currently picking up any signals."""
    with self.lock:
      return len(self.active_signals) > 0

  def _air_signal_start(self, data: bytes):
    """Called by MockMedium when a signal reaches this transceiver's antenna."""
    with self.lock:
      sig_id = id(data)
      self.active_signals[sig_id] = {"data": data, "collided": False}
      # If there are multiple active signals, they interfere and all collide
      if len(self.active_signals) > 1:
        for sig in self.active_signals.values():
          sig["collided"] = True

  def _air_signal_end(self, data: bytes):
    """Called by MockMedium when a signal finishes transmitting."""
    with self.lock:
      sig_id = id(data)
      if sig_id in self.active_signals:
        sig_info = self.active_signals.pop(sig_id)
        if not sig_info["collided"]:
          # Successfully received cleanly without overlap
          if self.callback:
            def delayed_callback(d):
              time.sleep(0.005)  # Simulated processing delay
              self.callback(d)
            threading.Thread(
                target=delayed_callback,
                args=(sig_info["data"],),
                daemon=True,
            ).start()
        else:
          # Signal was mangled by a collision. Pass garbage to simulate noise.
          garbage = bytearray(len(data))
          for i in range(len(garbage)):
            garbage[i] = random.randint(0, 255)
          if self.callback:
            def delayed_callback_garbage(d):
              time.sleep(0.005)
              self.callback(d)
            threading.Thread(
                target=delayed_callback_garbage,
                args=(bytes(garbage),),
                daemon=True,
            ).start()
