Raccoon
=======

Raccoon is a simple database engine in Java

- All changes are performed in a single global copy-on-write transaction
- All read/write operations are checksummed
- Optional compression with different levels applied to different types of blocks (index nodes, leaf nodes, blobs etc.)
- Optional encryption of the entire database
- Optional internal mirroring of important blocks (not ready)
