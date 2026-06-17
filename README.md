# Radio Mesh Network

A lightweight Python protocol for bringing up a peer-to-peer mesh network over raw radio transceivers. 

## Code Architecture

The codebase is split into modular layers:
1. `transceiver.py` & `mock_transceiver.py`: The physical layer. Represents the raw radio hardware, simulating collisions, hardware delays, and propagation distance.
2. `frame.py`: The data-link layer. Handles packing, unpacking, parsing, and verifying (CRC32) binary packets (`MeshFrame`).
3. `mesh_protocol.py`: The network layer. Handles the routing logic, route discovery, message ACKs, and deduplication.

## How it works (The Algorithms)

Our protocol uses two main algorithms to ensure messages reach their destination reliably across multiple devices:

1. **CSMA/CA (Carrier-Sense Multiple Access with Collision Avoidance)**: 
   Also known as "Listen Before Talk". Before a node transmits data, it listens to the antenna. If it hears another node talking, it waits for a random exponential backoff period before trying again. This drastically reduces radio wave collisions and allows the network to scale without scrambling packets.

2. **AODV Routing (Ad hoc On-Demand Distance Vector)**: 
   Instead of blindly flooding data packets everywhere, nodes establish precise paths on-demand. 
   - **Route Request (RREQ)**: When a node wants to send data, it floods a tiny RREQ packet. 
   - **Route Reply (RREP)**: When the destination hears it, it sends an RREP backwards along the discovered path. 
   - Once the path is locked in, the heavy `DATA` packets are sent *only* along that precise sequence of nodes. Intermediate nodes act as relays.

## Minimal Example

Here is how you can initialize the mock environment and send a message between two nodes.

```python
import time
from mock_transceiver import MockMedium, MockTransceiver
from mesh_protocol import MeshProtocol

# 1. Create the physical medium (Air) with a 3km range
medium = MockMedium(max_range_m=3000, bytes_per_sec=1000)

# 2. Setup Nodes
tx_a = MockTransceiver(medium, x=0, y=0, name="Node_A")
tx_b = MockTransceiver(medium, x=1000, y=0, name="Node_B")

proto_a = MeshProtocol(tx_a, "Node_A")
proto_b = MeshProtocol(tx_b, "Node_B")

# 3. Setup a callback to receive messages
def on_message(sender_id, payload):
    print(f"Received from {sender_id}: {payload.decode('utf-8')}")

proto_b.set_message_callback(on_message)

# 4. Send a message!
print("Sending message from A to B...")

# send_message will automatically run AODV discovery, transmit, and wait for an ACK
success = proto_a.send_message("Node_B", b"Hello, Mesh!")

if success:
    print("Message delivered and acknowledged!")
```
