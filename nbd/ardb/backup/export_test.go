package backup

import "testing"

func TestFetchStorageBlocks_SmallToBig(t *testing.T) {
	testFetchStorageBlocks(t, 8, 32)
}

func TestFetchStorageBlocks_Equal(t *testing.T) {
	testFetchStorageBlocks(t, 32, 32)
}

func TestFetchStorageBlocks_BigToSmall(t *testing.T) {
	testFetchStorageBlocks(t, 32, 8)
}

func testFetchStorageBlocks(t *testing.T, srsBS, dstBS int64) {
	// TODO
}
