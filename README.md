# Radio Mesh Network

A lightweight Python protocol for bringing up a peer-to-peer mesh network over raw radio transceivers. 

## Frame Format

Every packet on the wire follows this structure:

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

Total header size: **43 bytes** + payload + 4 bytes CRC.

## How it works

The protocol combines several techniques to deliver messages reliably across a network of nodes that can only talk to their immediate neighbors.

### CSMA/CA (Carrier-Sense Multiple Access with Collision Avoidance)

Also known as "Listen Before Talk". Before a node transmits, it listens to the antenna. If it hears another node talking, it waits for a random exponential backoff period before trying again. This drastically reduces radio wave collisions and allows the network to scale without scrambling packets.

### AODV Routing (Ad hoc On-Demand Distance Vector)

Instead of blindly flooding data packets everywhere, nodes establish precise paths on-demand:

- **Route Request (RREQ)**: When a node wants to send data to a destination it doesn't have a route to, it floods a small RREQ packet across the network.
- **Route Reply (RREP)**: When the destination hears the RREQ, it sends an RREP back along the discovered path.
- Once the path is established, DATA packets are sent *only* along that precise chain of nodes. Intermediate nodes act as relays.

Routes are cached and expire automatically (see Mobility-Aware Routing below). If a route expires or breaks (detected by a failed ACK), the node re-discovers a new path.

### End-to-End ACKs with Retries

When a node sends a unicast message, it waits for an acknowledgment (ACK) from the final destination. If the ACK doesn't arrive within a timeout, it clears the cached route and retries with a fresh route discovery. This provides reliable delivery — `send_message` returns `True` only when the destination has confirmed receipt.

### Message Deduplication

Every frame carries an `(origin, sequence_number, type)` tuple. Nodes track recently seen messages and silently drop duplicates. This is essential for RREQ flooding to work correctly — without it, a single RREQ would loop endlessly through the network.

### Route Discovery Coalescing

If multiple messages target the same destination before a route is established, only a single RREQ is broadcast. All pending senders wait on the same discovery and share the result, avoiding redundant floods.

### Mobility-Aware Routing

Each node declares itself as either **stationary** or **mobile** at creation time. This flag is carried in every frame header and used to make smarter routing decisions:

- **Adaptive route expiry**: Routes through stationary nodes are cached for 30 minutes, while routes through mobile nodes expire after just 1 minute. This avoids wasting bandwidth on repeated discoveries for stable links, while keeping routes fresh for nodes that move.
- **Route preference**: When multiple paths to a destination are discovered, the protocol prefers routes through stationary relays — even if they are up to 2 hops longer — because they are more stable. A 3-hop path through fixed infrastructure is more reliable than a 2-hop path through a moving device.

## Minimal Example

Here is how you can initialize the mock environment and send a message between two nodes.

```python
import asyncio
from mock_transceiver import MockMedium, MockTransceiver
from mesh_protocol import MeshProtocol

async def main():
    # 1. Create the physical medium (Air) with a 3km range
    medium = MockMedium(max_range_m=3000, bytes_per_sec=1000)

    # 2. Setup Nodes
    tx_a = MockTransceiver(medium, x=0, y=0, name="Node_A")
    tx_b = MockTransceiver(medium, x=1000, y=0, name="Node_B")

    proto_a = MeshProtocol(tx_a, "Node_A")  # stationary by default
    proto_b = MeshProtocol(tx_b, "Node_B", mobile=True)  # mobile node

    # 3. Setup a callback to receive messages
    def on_message(sender_id, payload):
        print(f"Received from {sender_id}: {payload.decode('utf-8')}")

    proto_b.set_message_callback(on_message)

    # 4. Send a message!
    print("Sending message from A to B...")

    # send_message will automatically run AODV discovery, transmit, and wait for an ACK
    success = await proto_a.send_message("Node_B", b"Hello, Mesh!")

    if success:
        print("Message delivered and acknowledged!")

asyncio.run(main())
```

