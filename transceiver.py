from abc import ABC, abstractmethod
from typing import Callable


class Transceiver(ABC):
  """
  Abstract interface for a radio transceiver.
  """

  @abstractmethod
  def broadcast(self, data: bytes) -> None:
    """Broadcasts a message."""
    pass

  @abstractmethod
  def set_receive_callback(self, callback: Callable[[bytes], None]) -> None:
    """
    Sets the callback function to be called when a message is received.
    The callback should take a single argument: the received bytes.
    """
    pass

  @abstractmethod
  def is_busy(self) -> bool:
    """Returns True if the transceiver detects radio activity."""
    pass
