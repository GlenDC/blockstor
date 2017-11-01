package temp

import (
	"testing"

	"github.com/zero-os/0-Disk/nbd/ardb"

	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-Disk/config"
)

func NewAssertions(t *testing.T) Assertions {
	return Assertions{require.New(t)}
}

type Assertions struct {
	*require.Assertions
}

func (assert Assertions) ServerIndex(expected interface{}) func(interface{}, error) {
	return func(value interface{}, err error) {
		assert.Assertions.NoError(err)
		assert.Assertions.Equal(expected, value)
	}
}

func (assert Assertions) NoServerIndex(expected error) func(interface{}, error) {
	return func(value interface{}, err error) {
		assert.Assertions.Equal(expected, err)
		assert.Assertions.Nil(value)
	}
}

func TestPrimarySlaveClusterPair_2_to_1(t *testing.T) {
	require := NewAssertions(t)

	slave := NewCluster(1, nil)
	require.NotNil(slave)

	primary := NewCluster(2, slave)
	require.NotNil(primary)

	// all good
	require.ServerIndex(PrimaryIndex(0))(primary.ServerIndex(0))
	require.ServerIndex(PrimaryIndex(1))(primary.ServerIndex(1))
	require.ServerIndex(PrimaryIndex(0))(primary.ServerIndex(2))
	require.ServerIndex(PrimaryIndex(1))(primary.ServerIndex(3))

	// bring 1 primary server offline
	// Result:
	// Primary: | OFFLINE | ONLINE |
	// Slave:   | ONLINE |
	require.NoError(primary.SetServerState(0, config.StorageServerStateOffline))

	// test now again
	require.ServerIndex(SlaveIndex(0))(primary.ServerIndex(0))
	require.ServerIndex(PrimaryIndex(1))(primary.ServerIndex(1))
	require.ServerIndex(SlaveIndex(0))(primary.ServerIndex(2))
	require.ServerIndex(PrimaryIndex(1))(primary.ServerIndex(3))

	// bring that primary server back online
	// Result:
	// Primary: | ONLINE | ONLINE |
	// Slave:   | ONLINE |
	require.NoError(primary.SetServerState(0, config.StorageServerStateOnline))

	// all good (again)
	require.ServerIndex(PrimaryIndex(0))(primary.ServerIndex(0))
	require.ServerIndex(PrimaryIndex(1))(primary.ServerIndex(1))
	require.ServerIndex(PrimaryIndex(0))(primary.ServerIndex(2))
	require.ServerIndex(PrimaryIndex(1))(primary.ServerIndex(3))

	// bring other primary server offline
	// Result:
	// Primary: | OFFLINE | ONLINE |
	// Slave:   | ONLINE |
	require.NoError(primary.SetServerState(1, config.StorageServerStateOffline))

	// test now again
	require.ServerIndex(PrimaryIndex(0))(primary.ServerIndex(0))
	require.ServerIndex(SlaveIndex(0))(primary.ServerIndex(1))
	require.ServerIndex(PrimaryIndex(0))(primary.ServerIndex(2))
	require.ServerIndex(SlaveIndex(0))(primary.ServerIndex(3))

	// bring last online primary server offline as well
	// Result:
	// Primary: | OFFLINE | OFFLINE |
	// Slave:   | ONLINE |
	require.NoError(primary.SetServerState(0, config.StorageServerStateOffline))

	// test now again
	require.ServerIndex(SlaveIndex(0))(primary.ServerIndex(0))
	require.ServerIndex(SlaveIndex(0))(primary.ServerIndex(1))
	require.ServerIndex(SlaveIndex(0))(primary.ServerIndex(2))
	require.ServerIndex(SlaveIndex(0))(primary.ServerIndex(3))

	// bring slave server offline as well
	// Result:
	// Primary: | OFFLINE | OFFLINE |
	// Slave:   | OFFLINE |
	require.NoError(slave.SetServerState(0, config.StorageServerStateOffline))

	// test now again
	require.NoServerIndex(ardb.ErrServerUnavailable)(primary.ServerIndex(0))
	require.NoServerIndex(ardb.ErrServerUnavailable)(primary.ServerIndex(1))
	require.NoServerIndex(ardb.ErrServerUnavailable)(primary.ServerIndex(2))
	require.NoServerIndex(ardb.ErrServerUnavailable)(primary.ServerIndex(3))

	// bring last primary server online again
	// Result:
	// Primary: | OFFLINE | ONLINE |
	// Slave:   | OFFLINE |
	require.NoError(primary.SetServerState(1, config.StorageServerStateOnline))

	// test now again
	require.NoServerIndex(ardb.ErrServerUnavailable)(primary.ServerIndex(0))
	require.ServerIndex(PrimaryIndex(1))(primary.ServerIndex(1))
	require.NoServerIndex(ardb.ErrServerUnavailable)(primary.ServerIndex(2))
	require.ServerIndex(PrimaryIndex(1))(primary.ServerIndex(3))

	// bring slave server back online
	// Result:
	// Primary: | OFFLINE | ONLINE |
	// Slave:   | ONLINE |
	require.NoError(slave.SetServerState(0, config.StorageServerStateOnline))

	// test now again
	require.ServerIndex(SlaveIndex(0))(primary.ServerIndex(0))
	require.ServerIndex(PrimaryIndex(1))(primary.ServerIndex(1))
	require.ServerIndex(SlaveIndex(0))(primary.ServerIndex(2))
	require.ServerIndex(PrimaryIndex(1))(primary.ServerIndex(3))

	// kill first primary server
	// Result:
	// Primary: | RIP | ONLINE |
	// Slave:   | ONLINE |
	require.NoError(primary.SetServerState(0, config.StorageServerStateRIP))

	// test now again
	require.ServerIndex(PrimaryIndex(1))(primary.ServerIndex(0))
	require.ServerIndex(PrimaryIndex(1))(primary.ServerIndex(1))
	require.ServerIndex(PrimaryIndex(1))(primary.ServerIndex(2))
	require.ServerIndex(PrimaryIndex(1))(primary.ServerIndex(3))

	// bring last available primary server offline
	// Result:
	// Primary: | RIP | OFFLINE |
	// Slave:   | ONLINE |
	require.NoError(primary.SetServerState(1, config.StorageServerStateOffline))

	// test now again
	require.ServerIndex(SlaveIndex(0))(primary.ServerIndex(0))
	require.ServerIndex(SlaveIndex(0))(primary.ServerIndex(1))
	require.ServerIndex(SlaveIndex(0))(primary.ServerIndex(2))
	require.ServerIndex(SlaveIndex(0))(primary.ServerIndex(3))

	// bring first primary server back from the dead
	// Result:
	// Primary: | ONLINE | OFFLINE |
	// Slave:   | ONLINE |
	require.NoError(primary.SetServerState(0, config.StorageServerStateOnline))

	// test now again
	require.ServerIndex(PrimaryIndex(0))(primary.ServerIndex(0))
	require.ServerIndex(SlaveIndex(0))(primary.ServerIndex(1))
	require.ServerIndex(PrimaryIndex(0))(primary.ServerIndex(2))
	require.ServerIndex(SlaveIndex(0))(primary.ServerIndex(3))
}
