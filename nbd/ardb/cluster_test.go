package ardb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/redisstub/ledisdb"
)

// TODO (part of https://github.com/zero-os/0-Disk/issues/543)
// add examples:
// ExampleNewCluster
// ExampleNewClusterPair

func cluster() StorageCluster {
	server := ledisdb.NewServer()
	cluster, err := NewUniCluster(config.StorageServerConfig{
		Address: server.Address(),
	}, nil)
	if err != nil {
		panic(err)
	}
	return cluster
}

func TestComputeServerIndex_MaxAvailability(t *testing.T) {
	require := require.New(t)

	const (
		serverCount = 13
		objectCount = 100
	)

	for objectIndex := int64(0); objectIndex < objectCount; objectIndex++ {
		index, err := ComputeServerIndex(serverCount, objectIndex,
			func(_ int64) (bool, error) { return true, nil })
		require.NoError(err)
		require.Equal(objectIndex%serverCount, index)
	}
}

func TestComputeServerIndex_ErrorAtEvenServerIndex(t *testing.T) {
	require := require.New(t)

	const (
		serverCount = 15
		objectCount = 111
	)

	for objectIndex := int64(0); objectIndex < objectCount; objectIndex++ {
		index, err := ComputeServerIndex(serverCount, objectIndex, func(i int64) (bool, error) {
			if i%2 == 0 {
				return false, ErrServerUnavailable
			}
			return true, nil
		})

		if (objectIndex%serverCount)%2 == 0 {
			require.Equal(ErrServerUnavailable, err)
		} else {
			require.NoError(err)
			require.Equal(objectIndex%serverCount, index)
		}
	}
}

func TestComputeServerIndex_OddIndexedServersDead(t *testing.T) {
	require := require.New(t)

	pred := func(i int64) (bool, error) {
		if i%2 == 1 {
			return false, nil
		}
		return true, nil
	}

	test := func(index, count, serverIndex int64, err error) {
		require.NoError(err)
		if index != -1 {
			require.Equal(serverIndex, index)
			return
		}
		// is any other serverIndex
		for i := int64(0); i < count; i += 2 {
			if i == serverIndex {
				return
			}
		}
		t.Fatal("serverIndex was odd")
	}

	count := int64(2)

	serverIndex, err := ComputeServerIndex(count, 0, pred)
	test(0, count, serverIndex, err)

	serverIndex, err = ComputeServerIndex(count, 1, pred)
	test(0, count, serverIndex, err)

	count++

	serverIndex, err = ComputeServerIndex(count, 1, pred)
	test(-1, count, serverIndex, err)

	serverIndex, err = ComputeServerIndex(count, 2, pred)
	test(-1, count, serverIndex, err)
}

func TestComputeServerIndex_SomeServersDead_SomeServersError(t *testing.T) {
	require := require.New(t)

	pred := func(i int64) (bool, error) {
		switch i {
		case 1:
			return false, nil
		case 2:
			return false, ErrServerUnavailable
		default:
			return true, nil
		}
	}

	const count = int64(3)

	test := func(index, serverIndex int64, err error) {
		if err == ErrServerUnavailable {
			return // this is possible
		}

		require.NoError(err)
		if index != -1 {
			require.Equal(serverIndex, index)
			return
		}
		// is any other serverIndex
		for i := int64(0); i < count; i += 2 {
			if i == serverIndex {
				return
			}
		}
		t.Fatal("serverIndex was odd")
	}

	testErr := func(_ int64, err error) {
		require.Equal(ErrServerUnavailable, err)
	}

	for i := int64(0); i < 30; i += 3 {
		serverIndex, err := ComputeServerIndex(count, i, pred)
		test(0, serverIndex, err)

		serverIndex, err = ComputeServerIndex(count, i+1, pred)
		test(-1, serverIndex, err)

		testErr(ComputeServerIndex(count, i+2, pred))
	}
}
