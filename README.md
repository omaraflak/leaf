# Radio Mesh Network

A lightweight Python library for establishing ad-hoc, peer-to-peer mesh networks over raw radio transceivers (such as LoRa). 

## Architecture Layers

### 1. Base Layer: `MeshProtocol`
The base layer manages point-to-point and multi-hop delivery of packets. It is designed to work in highly dynamic environments where nodes can move.

* **CSMA/CA (Carrier-Sense Multiple Access)**: Lists to the radio carrier before transmitting and implements random backoffs to prevent frame collisions.
* **AODV Routing**: Discovers routes on-demand using Route Request (RREQ) broadcasts and Route Reply (RREP) unicasts.
* **End-to-End ACKs**: Retransmits unicast packets if a confirmation ACK is not received back from the final destination.
* **Mobility-Aware Route Expiry**: Distinguishes between stationary and mobile nodes. Routes through stationary nodes last **30 minutes**, while routes through mobile nodes expire in **1 minute**.
* **Payload Constraints**: Restricts individual frames to a maximum size of **65,535 bytes** (governed by the 2-byte length field).

### 2. Transport Layer: `FragmentedMeshProtocol`
Since physical transceivers (e.g. Ebyte E32 LoRa) have very small internal serial buffers (often 512 bytes) and low-rate links, transmitting a single large packet will cause buffer overflows or extremely high packet loss rates.

`FragmentedMeshProtocol` wraps `MeshProtocol` to handle large transfers:
* **Automatic Segmentation**: Fragments arbitrary-sized data into small chunks (default `FRAGMENT_SIZE = 200` bytes).
* **High Efficiency**: A 10-byte fragment header tracks the packet state, supporting individual fragment acknowledgments and selective retries.
* **Automatic Purging**: Cleans up incomplete, timed-out chunks after 30 seconds to prevent resource leaks.

---

## Packet Formats

### 1. `MeshFrame` (Base Frame on the wire)

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

Total base frame overhead: **43 bytes** + 4 bytes CRC.

### 2. Fragment Header (Within `MeshFrame.payload`)

When using `FragmentedMeshProtocol`, the first **9 bytes** of the `MeshFrame.payload` are consumed by the fragment header:

| Field | Size | Type | Description |
|---|---|---|---|
| Message ID | 1 byte | Unsigned Char (`B`) | Unique message identifier (wraps at 256). |
| Chunk Index | 4 bytes | Unsigned Int (`I`) | Index of this chunk (0-indexed). |
| Total Chunks | 4 bytes | Unsigned Int (`I`) | Total number of chunks in this message. |

---

## Usage Examples

### Example 1: Direct Mesh Messaging (`MeshProtocol`)

Use this for sending small, lightweight payloads (under 65KB, or within physical hardware constraints) directly.

```python
import asyncio
from mock_transceiver import MockMedium, MockTransceiver
from mesh_protocol import MeshProtocol

async def main():
    medium = MockMedium(max_range_m=3000, bytes_per_sec=1000)

    # Setup transceivers
    tx_a = MockTransceiver(medium, x=0, y=0, name="Node_A")
    tx_b = MockTransceiver(medium, x=1000, y=0, name="Node_B")

    # Initialize protocol nodes
    proto_a = MeshProtocol(tx_a, "Node_A")
    proto_b = MeshProtocol(tx_b, "Node_B", mobile=True)

    # Listen for messages
    def on_message(sender_id, payload):
        print(f"Received from {sender_id}: {payload.decode('utf-8')}")

    proto_b.set_message_callback(on_message)

    # Send a message! (Triggers AODV discovery and waits for ACK)
    success = await proto_a.send_message("Node_B", b"Hello, Mesh!")
    if success:
        print("Message delivered and acknowledged!")

    proto_a.close()
    proto_b.close()

asyncio.run(main())
```

### Example 2: Sending Large Payloads (`FragmentedMeshProtocol`)

Use this to transmit large payloads (e.g. photos, log files, sensor dumps) safely over low-bandwidth physical radio modules (like LoRa modules with 512-byte serial buffers).

```python
import asyncio
from mock_transceiver import MockMedium, MockTransceiver
from fragmented_mesh import FragmentedMeshProtocol

async def main():
    # Setup medium with a faster transmission speed for large data simulations
    medium = MockMedium(max_range_m=3000, bytes_per_sec=50000)

    tx_a = MockTransceiver(medium, x=0, y=0, name="Node_A")
    tx_b = MockTransceiver(medium, x=1000, y=0, name="Node_B")

    # Setup nodes using the fragmented wrapper
    proto_a = FragmentedMeshProtocol(tx_a, "Node_A")
    proto_b = FragmentedMeshProtocol(tx_b, "Node_B")

    def on_message(sender_id, payload):
        print(f"Assembled and received {len(payload)} bytes from {sender_id}!")

    proto_b.set_message_callback(on_message)

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
