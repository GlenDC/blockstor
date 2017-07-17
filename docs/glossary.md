# Glossary

## [ A - J ]

### AGGMQ

The [Aggregation](#aggregation) Message Queue (AGGMQ) is used to queue [aggregations](#aggregation) in the [TLog](#tlog) server as they are waiting to be processed. See the [tlog docs](/docs/tlog/tlog.md) for more info.

### aggregation

An aggregation consists out of a group of [TLog](#tlog) sequences and some metadata (such as the [vdisk](#vdisk)'s identifier). See the [TLog docs](/docs/tlog/tlog.md) for more info. The aggregation definition can be found as a [Cap'n Proto][capnp] schema under [/tlog/schema/tlog_schema.capnp](/tlog/schema/tlog_schema.capnp).

### ARDB

ARDB (or "A Redis Database") is the collective term used for the underlying storage where the [persistent](#persistent) [vdisks](#vdisk)' [data](#data) and [metadata](#metadata) are stored. In production [redis][redis] is used, while [LedisDB][ledis] is used for test- and dev purposes. See the [NBD storage docs](/docs/nbd/storage/storage.md) for more info.

### backend

[NBD](#nbd) backend is the core implementation of all [volume drivers](#volume-driver) used via the [nbdserver](#nbd). It is used as glue between the [gonbdserver](/nbd/gonbdserver) and the actual underlying [nbd storage](#storage). See the [NBD docs](/docs/nbd/nbd.md) for more info.

### backup

todo

### block

All [vdisks](#vdisk)' [data](#data) is stored as blocks, with a dynamic size usually measured in bytes. 4096 KiB is the standard size for [boot](#boot) [vdisks](#vdisk). See the [NBD storage docs](/docs/nbd/storage/storage.md) for more info.

### boot

Boot [vdisk](#vdisk) is one of the available [vdisk](#vdisk) types. It uses the [deduped storage](#deduped) as its underlying [storage](#storage) type. [Data](#data) and [metadata](#metadata) are redundant by making use of the [TLog](#tlog) client. The boot type is meant for (OS) images, hence the name. See the [NBD docs](/docs/nbd/nbd.md) for more info.

### cache

Cache [vdisk](#vdisk) is one of the available [vdisk](#vdisk) types. It uses the [nondeduped storage](#nondeduped) as its underlying [storage](#storage) type. As its name suggests it is meant for caching. See the [NBD docs](/docs/nbd/nbd.md) for a more elaborabe overview.

### data

Data in the context of the 0-Disk refers to the [blocks](#block) of a [vdisk](#vdisk). See the [NBD docs](/docs/nbd/nbd.md) for more info.

### db

Database (db) [vdisk](#vdisk) is one of the available [vdisk](#vdisk) types. It uses the [nondeduped storage](#nondeduped) as its underlying [storage](#storage) type. [Data](#data) and [metadata](#metadata) are redundant by making use of the [TLog](#tlog) client. The boot type is meant for (OS) images, hence the name. See the [NBD docs](/docs/nbd/nbd.md) for more info.

### deduped

[Blocks](#block) stored in the deduped [storage](#storage) are only stored once, and are identified by their block [hash](#hash), which is referenced via its [metadata](#metadata). See the [deduped storage docs](/docs/nbd/storage/deduped.md) for more info.

### export

todo

### hash

A hash function is any function that can be used to map data of arbitrary size to data of fixed size. In the case of 0-Disk it can be assumed that the [blake2b][blake2b] cryptographic hashing algorithm is used, unless explicitly specified otherwise. A hash is the name for the fixed-sized data produced by the hashing algorithm.

### hotreload

Any 0-Disk config supports hot reloading as an optional feature, for where it is needed. Meaning that the config can be reloaded without having to restart the application using it. See the [config docs](/docs/config.md) for more info.

### import

todo

### index

Sequence index used to identify a [TLog](#tlog) sequence in order. The index is automatically asigned by the [TLog Server](/docs/tlog/server.md).

[Block](#block) index used to identify a [vdisk](#vdisk)'s [block](#block). The index can be calculated using the formula `i = position % blockSize`, where both the position and blockSize are expressed in bytes. 

[LBA](#lba) shard (todo: better name for this) index used to identify an [LBA](#lba) shard. The index can be calculated using the formula `i = position % blockSize % 128`, where both the position and blockSIze are expressed in bytes.

## [ K - T ]

### LBA

todo

### local

### log

Info and debug logs... todo

OS logs (real name =?) ... (communicate with 0-message-service-shit (name?))... todo

[Transaction Logs](#tlog)... todo

### message

### metadata

### NBD

### nondeduped

[Blocks](#block) stored in the nondeduped [storage](#storage) are stored, identified directly using their [block](#block) [index](#index). Because of this no [metadata](#metadata) is required for the storage of nondeduped [blocks](#block). See the [nondeduped storage docs](/docs/nbd/storage/nondeduped.md) for more info.

### persistent

todo

### player

todo

### pool

todo

### redundant

todo

### remote

todo

### restore

[Redundant](#redundant) content can be restored using the [TLog Player](#player) to any given time or sequence in tracked history, usually done so using the [zeroctl](#zeroctl) tool.

### rollback

[Redundant](#redundant) content can be rolled back to any given time or sequence in tracked history, usually done so using the [zeroctl](#zeroctl) tool. See the [tlog docs](/docs/tlog/tlog.md) for more info.

### semideduped

Semi Deduped storage is a hybrid storage, using ([local](#local)) read-only [deduped storage](#deduped) to store the ([remote](#remote)) [template](#template) content, and ([local](#local)) [nondeduped storage](#nondeduped) to store any content (modifications) written by the user. See the [semideduped storage docs](/docs/nbd/storage/semideduped.md) for more info.

### shard

todo: come up with a better name for an "LBA Shard"

### slave

todo

### storage

todo

### template

todo

### tlog

todo

## [ U - Z ]

### vdisk

todo

### volume driver

todo

### zeroctl

The 0-Disk command line tool, used to manage [vdisks](#vdisk). See the [zeroctl docs](/docs/zeroctl/zeroctl.md) for more info.

[redis]: http://redis.io
[ledis]: http://ledisdb.com
[capnp]: https://capnproto.org
[blake2b]: github.com/minio/blake2b-simd
