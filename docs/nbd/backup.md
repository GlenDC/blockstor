# Backup

The backup module allows you to [export](#export) a [snapshot][snapshot] of a [vdisk][vdisk] onto a storage server in a secure (encrypted) and efficient (deduped) manner. In production and for most purposes the storage server is assumed to be an FTP server. This [snapshot][snapshot] can than be restored (see: [import](#import)) at a later time as a new [vdisk][vdisk] or to overwrite an existing [vdisk][vdisk].

All blocks stored for a given [snapshot][snapshot] are deduped, and the deduped map is stored in the `/backups` subdir, mapping all indices to the deduped block's hashes. The deduped map as well as the deduped blocks are compressed AND encrypted. The compression happens first and can be done using either the default [LZ4][lz4] algorithm, or the slower but stronger [XZ][xz] compression algorithm. Once the content is compressed it is encrypted using the [AES stream cipher][aes] algorithm in [GCM mode][gcm] using the given private (32 byte) key. When [importing](#import) a [snapshot][snapshot] decryption happens first, and decompression second, the reverse process of [exporting](#export).

For more information about The Golang implementation of this backup module, and how to use it, you can read the [backup Godocs information][backupGodocs].

## Deduped Map

All blocks of a [vdisk][vdisk]'s [snapshot][snapshot] are deduped. This deduplication is done using the [hash][hash] of the compressed+encrypted block. Meaning that if you have multiple blocks (in one or multiple [snapshots][snapshot]) which are compressed using the same compression algorithm and encrypted using the same private key, will only be stored once.

The deduped map is encoded using the [bencoding][bencode] encoding format. It is choosen for its simplicity and efficiency. The encoded version of the deduped map is also compressed and encrypted before writing it to the storage server, using the same compression algorithm and private key used for the compression and encryption of the (deduped) blocks.

As the content is stored and referenced using its hash, and the hash is stored in an encrypted deduped map, the content is also guaranteed to stay untouched and correctly linked to the block index it was assigned to at serialization time.

## Storage

In production and for most use cases the [snapshots][snapshot] are stored onto a given FTP server. A [snapshot][snapshot] consists out of (deduped) block data, and a deduped map. The deduped map links each block index with that block's hash.

The deduped map is stored using the [snapshot][snapshot]'s identifier as its name under the `backups/` subdir of the root directory.

All deduped blocks are stored under `/XX/YY/ZZZZZZZZZZZZZZZZZZZZZZZZZZZZ` of the root directory. Thus all blocks are stored in a 2-layer deep directory structure, where `XX` are the first 2 bytes of the block's [hash][hash] and the name of the first directory. `YY` represents the 3rd and 4th bytes of the block's [hash][hash]. `ZZZZZZZZZZZZZZZZZZZZZZZZZZZZ`, the last 28 bytes of the block's [hash][hash] is the name of the block itself.

## Export

The backup module provides a global `Export` function which allows you to export a given [vdisk][vdisk]. While it is possible to do so via the public module API, it is instead usually done using the [zeroctl tool][zeroctl] instead. It should be noted that a [vdisk][vdisk] should first be unmounted before being exported. While there is no protection against this, failing to do so will result in unexpected behavior.

The export process is straightforward and can be summarized as follows:

1. All block indices are collected from the ARDB cluster, ordered from smallest to biggest;
2. The deduped map is loaded if it already existed, and newly created if it didn't;
2. All content is written in a parallel and streamlined fashion:
    1. All blocks are read in parallel from the ARDB storage;
    2. The blocks are ordered in a single goroutine and sized to the correct export block size;
    3. The newly sized blocks are all compressed, encrypted and stored in parallel;
    4. If the hash of the encrypted content's hash is alreayd mapped to the block index in question, it is not written to the storage server, as it is assumed in such case that the block already exists on the storage server;

Check out [the zeroctl export command documentation][export] for more information on how to export a [vdisk][vdisk] yourself.

## Import

The backup module provides a global `Import` function which allows you to import a given [vdisk][vdisk]'s [snapshot][snapshot], earlier created by [exporting](#export) a [vdisk][vdisk]. While it is possible to do so via the public module API, it is instead usually done using the [zeroctl tool][zeroctl] instead. It should be noted that a [vdisk][vdisk] shouldn't exist before it can be imported. However if it it does exist the [zeroctl][zeroctl] can destroy it while it's at it.

The import process is straightforward and can be summarized as follows:

1. The deudped map is loaded from disk, and all its hash and index mappings are collected;
2. All content is read and optionally stored in a parallel and streamlined fashion:
    1. All deduped (snapshot) blocks are read in parallel from the given storage (FTP) server;
    2. If the read block is correct and untouched, it is decrypted and decompressed, to send for storage into the ARDB cluster;
    3. The blocks are ordered in a single goroutine and sized to the correct import block size;
    4. The newly sized blocks are all stored in the ARDB storage cluster in a parallel and streamlined fashion;

Check out [the zeroctl import command documentation][import] for more information on how to import a [vdisk][vdisk] yourself.

[vdisk]: /docs/glossary.md#vdisk
[snapshot]: /docs/glossary.md#snapshot
[hash]: /docs/glossary.md#hash
[lz4]: https://godoc.org/github.com/pierrec/lz4
[xz]: https://godoc.org/github.com/ulikunitz/xz
[aes]: https://godoc.org/crypto/aes
[gcm]: https://godoc.org/crypto/cipher#NewGCM
[bencode]: https://godoc.org/github.com/zeebo/bencode

[zeroctl]: /docs/zeroctl/zeroctl.md
[export]: /docs/zeroctl/commands/export.md
[import]: /docs/zeroctl/commands/import.md

[backupGodocs]: https://godoc.org/github.com/zero-os/0-Disk/nbd/ardb/backup