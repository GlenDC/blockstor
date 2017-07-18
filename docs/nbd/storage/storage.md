# Storage

The storage types define how a [persistent][persistent] [vdisk][vdisk]'s [data (1)][data] and [metadata (1,2,3)][metadata] is stored in an [ARDB cluster][ardb]. On top of that it also coordinate with any other optional services as the defined by the specific storage type.

![NBD Storage Overview](/docs/assets/nbd_storage_overview.png)

The [NBD Backend][backend] mainly converts the position in bytes into a [block index (2)][index], and delegates most of the actual storage work to the underlying storage type.

## Storage Types

### Deduped Storage

Deduped Storage stores any given [block][block] only once. Meaning it has no duplicated [blocks][block], hence its name. All [blocks][block] are identified by their [hash][hash], and an [LBA][lba] scheme is used to map each [block index (2)][index] to the correct [block][block] [hash][hash].

See the [deduped storage docs](/docs/nbd/storage/deduped.md) for more information.

The code for this storage type can be found in [/nbdserver/ardb/deduped.go](/nbdserver/ardb/deduped.go).

### Non Deduped Storage

summary

link to docs and link to code

### Semi Deduped Storage

summary

link to docs and link to code

### TLog Storage

summary

link to docs and link to code

[backend]: /docs/glossary.md#backend
[persistent]: /docs/glossary.md#persistent
[vdisk]: /docs/glossary.md#vdisk
[ardb]: /docs/glossary.md#ardb
[data]: /docs/glossary.md#data
[metadata]: /docs/glossary.md#metadata
[index]: /docs/glossary.md#index
[lba]: /docs/glossary.md#lba
[block]: /docs/glossary.md#block
[hash]: /docs/glossary.md#hash

