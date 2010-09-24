===============
Design document
===============

Architectural design document for developers

GearmanConnectionManager - Bridges low-level I/O <-> command handlers
=====================================================================
* Only class that an API user should directly interact with
* Manages all I/O: polls connections, reconnects failed connections, etc...
* Forwards commands between Connections <-> CommandHandlers
* Manages multiple Connections and multple CommandHandlers
* Manages global state of an interaction with Gearman (global job lock)

GearmanConnection - Manages low-level I/O
=========================================
* A single connection between a client/worker and a server
* Thinly wrapped socket that can reconnect
* Converts binary strings <-> Gearman commands
* Manages in/out data buffers for socket-level operations
* Manages in/out command buffers for gearman-level operations

GearmanCommandHandler - Manages commands
========================================
* Represents the state machine of a single GearmanConnection
* 1-1 mapping to a GearmanConnection (via GearmanConnectionManager)
* Sends/receives commands ONLY - does no buffering
* Handles all command generation / interpretation

