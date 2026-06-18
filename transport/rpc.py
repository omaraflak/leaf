import abc
import asyncio
import inspect
import logging
import struct
from typing import Any, Callable, TypeVar, get_type_hints

from transport.fragmented_mesh import FragmentedMesh

logger = logging.getLogger("leaf.rpc")


# Private Message Types
_MSG_TYPE_REQUEST = 1
_MSG_TYPE_RESPONSE = 2


# Decorator to register RPC methods
def register(func: Callable[..., Any]) -> Callable[..., Any]:
  func._is_rpc_method = True  # type: ignore
  return func


def _pack_request(request_id: int, method_name: str, payload: bytes) -> bytes:
  method_bytes = method_name.encode("utf-8")
  if len(method_bytes) > 65535:
    raise ValueError("Method name too long")
  header = struct.pack("!BIH", _MSG_TYPE_REQUEST,
                       request_id, len(method_bytes))
  return header + method_bytes + payload


def _unpack_request(data: bytes) -> tuple[int, str, bytes]:
  if len(data) < 7:
    raise ValueError("Request data too short")
  msg_type, request_id, method_len = struct.unpack("!BIH", data[:7])
  if msg_type != _MSG_TYPE_REQUEST:
    raise ValueError(f"Invalid message type for request: {msg_type}")
  if len(data) < 7 + method_len:
    raise ValueError("Request data incomplete for method name")
  method_name = data[7: 7 + method_len].decode("utf-8")
  payload = data[7 + method_len:]
  return request_id, method_name, payload


def _pack_response(request_id: int, success: bool, payload_or_error: bytes) -> bytes:
  status = 0 if success else 1
  header = struct.pack("!BIB", _MSG_TYPE_RESPONSE, request_id, status)
  return header + payload_or_error


def _unpack_response(data: bytes) -> tuple[int, bool, bytes]:
  if len(data) < 6:
    raise ValueError("Response data too short")
  msg_type, request_id, status = struct.unpack("!BIB", data[:6])
  if msg_type != _MSG_TYPE_RESPONSE:
    raise ValueError(f"Invalid message type for response: {msg_type}")
  success = status == 0
  payload_or_error = data[6:]
  return request_id, success, payload_or_error


class Message(abc.ABC):
  """Abstract interface for all RPC requests and responses."""

  @abc.abstractmethod
  def serialize(self) -> bytes:
    """Serializes the message to bytes."""
    pass

  @classmethod
  @abc.abstractmethod
  def deserialize(cls, data: bytes) -> "Message":
    """Deserializes the message from bytes."""
    pass


class MeshRpcServer:
  """RPC Server running on top of FragmentedMesh."""

  def __init__(self, mesh: FragmentedMesh):
    self.mesh = mesh
    self._methods: dict[str, Callable[..., Any]] = {}
    self._request_classes: dict[str, type[Message]] = {}
    self._terminated = asyncio.Event()
    self._register_methods()
    self.mesh.add_message_listener(self._on_mesh_message)

  async def wait_for_termination(self):
    await self._terminated.wait()

  def close(self):
    self._terminated.set()
    self.mesh.remove_message_listener(self._on_mesh_message)

  def _register_methods(self):
    for name, attr in inspect.getmembers(self, predicate=inspect.ismethod):
      if getattr(attr, "_is_rpc_method", False):
        sig = inspect.signature(attr)
        params = list(sig.parameters.keys())
        if len(params) != 1:
          raise ValueError(
              f"RPC method '{name}' must have exactly one parameter (excluding"
              f" 'self'). Got: {params}"
          )
        hints = get_type_hints(attr)
        param_name = params[0]
        request_class = hints.get(param_name)
        if request_class is None or not issubclass(request_class, Message):
          raise TypeError(
              f"RPC method '{name}' parameter '{param_name}' must be annotated"
              f" with a subclass of Message. Got: {request_class}"
          )
        self._methods[name] = attr
        self._request_classes[name] = request_class
        logger.info(
            "Registered RPC method: %s (%s)", name, request_class.__name__
        )

  def _on_mesh_message(self, sender_id: str, payload: bytes):
    if len(payload) >= 7 and payload[0] == _MSG_TYPE_REQUEST:
      asyncio.create_task(self._handle_request(sender_id, payload))

  async def _handle_request(self, sender_id: str, payload: bytes):
    request_id = 0
    method_name = "unknown"
    try:
      request_id, method_name, req_payload = _unpack_request(payload)
      if method_name not in self._methods:
        raise AttributeError(f"Method '{method_name}' not found")

      method = self._methods[method_name]
      req_class = self._request_classes[method_name]

      # Deserialize request
      request_obj = req_class.deserialize(req_payload)

      # Invoke method
      response_obj = await method(request_obj)

      if not isinstance(response_obj, Message):
        raise TypeError(
            f"RPC method '{method_name}' must return a Message. Got:"
            f" {type(response_obj)}"
        )

      resp_payload = response_obj.serialize()
      success = True
    except Exception as e:
      logger.exception("Error executing RPC method '%s'", method_name)
      # Sanitize error message (TODO: security)
      resp_payload = str(e).encode("utf-8")
      success = False

    try:
      response_data = _pack_response(request_id, success, resp_payload)
      await self.mesh.send_message(sender_id, response_data)
    except Exception as e:
      logger.error("Failed to send RPC response to %s: %s", sender_id, e)


T = TypeVar("T", bound=Message)


class MeshRpcClient:
  """RPC Client running on top of FragmentedMesh."""

  def __init__(self, mesh: FragmentedMesh, server_node_id: str):
    self.mesh = mesh
    self.server_node_id = server_node_id
    self._next_request_id = 0
    self._pending_requests: dict[int, asyncio.Future] = {}
    self.mesh.add_message_listener(self._on_mesh_message)

  async def call(
      self,
      method_name: str,
      request: Message,
      response_class: type[T],
      timeout: float = 10.0,
  ) -> T:
    req_id = self._next_request_id
    self._next_request_id = (self._next_request_id + 1) % (2**32)

    payload = request.serialize()
    request_data = _pack_request(req_id, method_name, payload)

    future = asyncio.get_running_loop().create_future()
    self._pending_requests[req_id] = future

    success = await self.mesh.send_message(
        self.server_node_id, request_data, timeout=timeout
    )
    if not success:
      self._pending_requests.pop(req_id, None)
      raise IOError("Failed to send RPC request through mesh")

    try:
      success, payload_or_error = await asyncio.wait_for(future, timeout=timeout)
    except asyncio.TimeoutError:
      self._pending_requests.pop(req_id, None)
      raise TimeoutError(f"RPC call to {method_name} timed out")

    if not success:
      raise RuntimeError(f"RPC error: {payload_or_error.decode('utf-8')}")

    return response_class.deserialize(payload_or_error)  # type: ignore

  def close(self):
    for fut in self._pending_requests.values():
      if not fut.done():
        fut.cancel()
    self._pending_requests.clear()
    self.mesh.remove_message_listener(self._on_mesh_message)

  def _on_mesh_message(self, sender_id: str, payload: bytes):
    if len(payload) >= 6 and payload[0] == _MSG_TYPE_RESPONSE:
      try:
        request_id, success, payload_or_error = _unpack_response(payload)
        future = self._pending_requests.pop(request_id, None)
        if future and not future.done():
          future.set_result((success, payload_or_error))
      except Exception as e:
        logger.error("Error unpacking RPC response: %s", e)
