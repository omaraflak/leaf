# Radio Mesh Network

A lightweight Python library for establishing ad-hoc, peer-to-peer mesh networks over raw radio transceivers (such as LoRa). 

## Architecture Layers

### 1. Base Layer: `Mesh`
The base layer manages point-to-point and multi-hop delivery of packets. It is designed to work in highly dynamic environments where nodes can move.

* **CSMA/CA (Carrier-Sense Multiple Access)**: Lists to the radio carrier before transmitting and implements random backoffs to prevent frame collisions.
* **AODV Routing**: Discovers routes on-demand using Route Request (RREQ) broadcasts and Route Reply (RREP) unicasts.
* **End-to-End ACKs**: Retransmits unicast packets if a confirmation ACK is not received back from the final destination.
* **Mobility-Aware Route Expiry**: Distinguishes between stationary and mobile nodes. Routes through stationary nodes last **30 minutes**, while routes through mobile nodes expire in **1 minute**.
* **Payload Constraints**: Restricts individual frames to a maximum size of **65,535 bytes** (governed by the 2-byte length field).

#### Packet Format: `MeshFrame`

| Field | Size | Description |
|---|---|---|
| Magic | 2 bytes | Frame delimiter (`0xAABB`). Used to locate frame boundaries in a byte stream. |
| Type | 1 byte | Frame type: `DATA` (1), `ACK` (2), `RREQ` (3), `RREP` (4). |
| TTL | 1 byte | Time to live. Decremented at each hop; frame is dropped when it reaches 0. |
| Seq | 4 bytes | Sequence number. Monotonically increasing per node, used for deduplication and ACK matching. |
| OrigSrc | 8 bytes | Node ID of the original sender. |
| FinalDest | 8 bytes | Node ID of the final destination (or `0xFF * 8` for broadcast). |
| Transmitter | 8 bytes | Node ID of the last-hop transmitter (changes at each relay). |
| NextHop | 8 bytes | Node ID of the intended next-hop recipient (or `0xFF * 8` for broadcast). |
| Flags | 1 byte | Bit field. Bit 0: transmitter is stationary (`STATIONARY = 0x00`) or mobile (`MOBILE = 0x01`). |
| PayloadLen | 2 bytes | Length of the payload in bytes. |
| Payload | variable | Application data (for `DATA`/`ACK`) or target node ID (for `RREQ`/`RREP`). |
| CRC32 | 4 bytes | CRC-32 checksum over the header + payload (excludes the CRC field itself). |

Total base frame overhead: **47 bytes**.

```python
import asyncio
from core.mock_transceiver import MockMedium, MockTransceiver
from core.mesh import Mesh

async def main():
    medium = MockMedium(max_range_m=3000, bytes_per_sec=1000)

    # Setup transceivers
    tx_a = MockTransceiver(medium, x=0, y=0, name="Node_A")
    tx_b = MockTransceiver(medium, x=1000, y=0, name="Node_B")

    # Initialize protocol nodes
    proto_a = Mesh(tx_a, "Node_A")
    proto_b = Mesh(tx_b, "Node_B", mobile=True)

    # Listen for messages
    def on_message(sender_id, payload):
        print(f"Received from {sender_id}: {payload.decode('utf-8')}")

    proto_b.add_message_listener(on_message)

    # Send a message! (Triggers AODV discovery and waits for ACK)
    success = await proto_a.send_message("Node_B", b"Hello, Mesh!")
    if success:
        print("Message delivered and acknowledged!")

    proto_a.close()
    proto_b.close()

asyncio.run(main())
```

### 2. Transport Layer: `FragmentedMesh`
Since physical transceivers (e.g. Ebyte E32 LoRa) have very small internal serial buffers (often 512 bytes) and low-rate links, transmitting a single large packet will cause buffer overflows or extremely high packet loss rates.

`FragmentedMesh` wraps `Mesh` to handle large transfers:
* **Automatic Segmentation**: Fragments arbitrary-sized data into small chunks (default `FRAGMENT_SIZE = 200` bytes).
* **High Efficiency**: A 10-byte fragment header tracks the packet state, supporting individual fragment acknowledgments and selective retries.
* **Automatic Purging**: Cleans up incomplete, timed-out chunks after 30 seconds to prevent resource leaks.

#### Fragment Header (Within `MeshFrame.payload`)

When using `FragmentedMesh`, the first **9 bytes** of the `MeshFrame.payload` are consumed by the fragment header:

| Field | Size | Type | Description |
|---|---|---|---|
| Message ID | 1 byte | Unsigned Char (`B`) | Unique message identifier (wraps at 256). |
| Chunk Index | 4 bytes | Unsigned Int (`I`) | Index of this chunk (0-indexed). |
| Total Chunks | 4 bytes | Unsigned Int (`I`) | Total number of chunks in this message. |

Fragment header overhead: **9 bytes**. Cumulated with base frame: **56 bytes**.

```python
import asyncio
from core.mock_transceiver import MockMedium, MockTransceiver
from transport.fragmented_mesh import FragmentedMesh

async def main():
    # Setup medium with a faster transmission speed for large data simulations
    medium = MockMedium(max_range_m=3000, bytes_per_sec=50000)

    tx_a = MockTransceiver(medium, x=0, y=0, name="Node_A")
    tx_b = MockTransceiver(medium, x=1000, y=0, name="Node_B")

    # Setup nodes using the fragmented wrapper
    proto_a = FragmentedMesh(tx_a, "Node_A")
    proto_b = FragmentedMesh(tx_b, "Node_B")

    def on_message(sender_id, payload):
        print(f"Assembled and received {len(payload)} bytes from {sender_id}!")

    proto_b.add_message_listener(on_message)

    # Send a payload that exceeds MTU limits (e.g., 15 KB)
    large_payload = b"Large raw data block... " * 600

    print("Sending fragmented data...")
    # This automatically splits the 15KB data into ~75 fragments of 200 bytes
    success = await proto_a.send_message("Node_B", large_payload)
    if success:
        print("All fragments delivered and verified!")

    proto_a.close()
    proto_b.close()

asyncio.run(main())
```

### 3. RPC Layer: `MeshRpcServer` & `MeshRpcClient`
Built on top of `FragmentedMesh`, the RPC layer allows nodes to register asynchronous methods and invoke them remotely.

* **Decorator-Based Registration**: Methods decorated with `@register` are automatically discovered and exposed as RPC endpoints.
* **Typed Messages**: Request and response types extend the `Message` abstract class, enforcing explicit serialization.
* **Concurrent Calls**: Multiple RPC calls can be in-flight simultaneously, matched by request ID.

#### Packet Format: RPC Request (Within `FragmentedMesh` payload)

| Field | Size | Type | Description |
|---|---|---|---|
| MsgType | 1 byte | Unsigned Char (`B`) | Message type (`1` = Request). |
| RequestID | 1 byte | Unsigned Char (`B`) | Unique request identifier for matching responses (wraps at 256). |
| MethodNameLen | 1 byte | Unsigned Char (`B`) | Length of the method name in bytes. |
| MethodName | variable | UTF-8 string | Name of the RPC method to invoke. |
| Payload | variable | bytes | Serialized `Message` request body. |

#### Packet Format: RPC Response (Within `FragmentedMesh` payload)

| Field | Size | Type | Description |
|---|---|---|---|
| MsgType | 1 byte | Unsigned Char (`B`) | Message type (`2` = Response). |
| RequestID | 1 byte | Unsigned Char (`B`) | Matches the request that triggered this response. |
| Status | 1 byte | Unsigned Char (`B`) | `0` = success, `1` = error. |
| Payload | variable | bytes | Serialized `Message` response body (on success) or UTF-8 error string (on failure). |

RPC header overhead: **3 bytes** (+ method name for requests). Cumulated with base frame + fragment header: **59 bytes**.

```python
import asyncio
import json
from core.mock_transceiver import MockMedium, MockTransceiver
from transport.fragmented_mesh import FragmentedMesh
from transport.rpc import Message, MeshRpcServer, MeshRpcClient, register

# Define RPC request/response messages
class MyRequest(Message):
    def __init__(self, message: str):
        self.message = message

    def serialize(self) -> bytes:
        return json.dumps({"msg": self.message}).encode("utf-8")

    @classmethod
    def deserialize(cls, data: bytes) -> "MyRequest":
        parsed = json.loads(data.decode("utf-8"))
        return cls(parsed["msg"])

class MyResponse(Message):
    def __init__(self, reply: str):
        self.reply = reply

    def serialize(self) -> bytes:
        return json.dumps({"reply": self.reply}).encode("utf-8")

    @classmethod
    def deserialize(cls, data: bytes) -> "MyResponse":
        parsed = json.loads(data.decode("utf-8"))
        return cls(parsed["reply"])

# Implement the Server
class MyServer(MeshRpcServer):
    @register
    async def echo(self, request: MyRequest) -> MyResponse:
        print(f"Server received: {request.message}")
        return MyResponse(f"Echo: {request.message}")

async def main():
    medium = MockMedium(max_range_m=3000, bytes_per_sec=10000)

    # Node Setup
    tx_server = MockTransceiver(medium, x=0, y=0, name="ServerNode")
    tx_client = MockTransceiver(medium, x=1000, y=0, name="ClientNode")

    mesh_server = FragmentedMesh(tx_server, "ServerNode")
    mesh_client = FragmentedMesh(tx_client, "ClientNode")

    # Start Server
    server = MyServer(mesh_server)

    # Create Client
    client = MeshRpcClient(mesh_client, "ServerNode")

    # Call method
    response = await client.call("echo", MyRequest("Hello World!"), MyResponse)
    print(f"Client received response: {response.reply}")

    server.close()
    client.close()
    mesh_server.close()
    mesh_client.close()

asyncio.run(main())
```
