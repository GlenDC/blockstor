# NBD Server

TODO: Update

NBD, abbreviation for Network Block Device, is the lightweight block access protocol used in a Zero-OS cluster to implement block storage.

A NBD Server actually implements the NBD protocol. For each virtual machine, one NBD Server will be created. Each of these NBD Severs, or volume driver servers, runs in a separate container, and depends on another container that implements the TLOG Server.

![Architecture](block-storage-architecture.png)

Next:
- [Building your NBD Server](building.md)

## Possible Failure Scenarios

todo...