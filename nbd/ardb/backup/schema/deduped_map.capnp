using Go = import "/go.capnp";
@0xdf6eb9a04e9bc070;
$Go.package("schema");
$Go.import("github.com/zero-os/0-Disk/nbd/ardb/backup/schema");

struct DedupedMap {
	hashes @0 :List(IndexHashPair);
}

struct IndexHashPair {
	hash @0 :Data;
	index @1 :Int64;
}
