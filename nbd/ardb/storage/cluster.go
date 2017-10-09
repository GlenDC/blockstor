package storage

import (
	"context"
	"io"
	"net"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
)

// NewTemplateCluster creates a new TemplateCluster.
// See `TemplateCluster` for more information.
func NewTemplateCluster(ctx context.Context, vdiskID string, cs config.Source) (*TemplateCluster, error) {
	pool := ardb.NewPool(nil)
	cluster, err := ardb.NewCluster(nil, pool)
	if err != nil {
		pool.Close()
		return nil, err
	}

	templateCluster := &TemplateCluster{
		vdiskID: vdiskID,
		cluster: cluster,
		pool:    pool,
	}
	err = templateCluster.spawnConfigReloader(ctx, cs)
	if err != nil {
		templateCluster.Close()
		return nil, err
	}

	return templateCluster, nil
}

// TemplateCluster creates a template cluster using a config source.
// It supports hot reloading of the configuration,
// as well as the fact that the Cluster might not contain any servers at all.
type TemplateCluster struct {
	vdiskID string
	cluster *ardb.Cluster
	pool    *ardb.Pool
	cancel  context.CancelFunc
}

// Do implements StorageCluster.Do
func (tsc *TemplateCluster) Do(action ardb.StorageAction) (reply interface{}, err error) {
	cfg, err := tsc.cluster.Config()
	if err != nil {
		return nil, err
	}

	conn, err := tsc.pool.Dial(cfg.Config)
	if err == nil {
		defer conn.Close()
		reply, err = action.Do(conn)
		if err == nil {
			return reply, nil
		}
	}

	status := mapErrorToBroadcastStatus(err)
	log.Broadcast(
		status,
		log.SubjectStorage,
		log.ARDBServerTimeoutBody{
			Address:  cfg.Config.Address,
			Database: cfg.Config.Database,
			Type:     log.ARDBTemplateServer,
			VdiskID:  tsc.vdiskID,
		},
	)
	return nil, err
}

// DoFor implements StorageCluster.DoFor
func (tsc *TemplateCluster) DoFor(objectIndex int64, action ardb.StorageAction) (reply interface{}, err error) {
	cfg, err := tsc.cluster.ConfigFor(objectIndex)
	if err != nil {
		return nil, err
	}

	conn, err := tsc.pool.Dial(cfg.Config)
	if err == nil {
		defer conn.Close()
		reply, err = action.Do(conn)
		if err == nil {
			return reply, nil
		}
	}

	status := mapErrorToBroadcastStatus(err)
	log.Broadcast(
		status,
		log.SubjectStorage,
		log.ARDBServerTimeoutBody{
			Address:  cfg.Config.Address,
			Database: cfg.Config.Database,
			Type:     log.ARDBTemplateServer,
			VdiskID:  tsc.vdiskID,
		},
	)
	return nil, err
}

// Close any open resources
func (tsc *TemplateCluster) Close() error {
	tsc.cancel()
	tsc.pool.Close()
	return nil
}

func (tsc *TemplateCluster) spawnConfigReloader(ctx context.Context, cs config.Source) error {
	ctx, tsc.cancel = context.WithCancel(context.Background())

	vdiskNBDRefCh, err := config.WatchVdiskNBDConfig(ctx, cs, tsc.vdiskID)
	if err != nil {
		return err
	}

	vdiskNBDConfig := <-vdiskNBDRefCh
	templateClusterID := vdiskNBDConfig.TemplateStorageClusterID

	var templateClusterCfg config.StorageClusterConfig
	var templateClusterCh <-chan config.StorageClusterConfig

	templateCtx, templateCancel := context.WithCancel(ctx)

	if templateClusterID != "" {
		templateClusterCh, err = config.WatchStorageClusterConfig(
			templateCtx, cs, vdiskNBDConfig.TemplateStorageClusterID)
		if err != nil {
			templateCancel()
			return err
		}

		templateClusterCfg = <-templateClusterCh
		err = tsc.cluster.SetStorageconfig(templateClusterCfg)
		if err != nil {
			templateCancel()
			return err
		}
	}

	go func() {
		defer templateCancel()

		for {
			select {
			case <-ctx.Done():
				return

			case vdiskNBDConfig = <-vdiskNBDRefCh:
				if vdiskNBDConfig.TemplateStorageClusterID == templateClusterID {
					continue
				}
				if vdiskNBDConfig.TemplateStorageClusterID == "" {
					templateClusterID = ""
					templateClusterCh = nil
					templateCancel()
					continue
				}

				templateCtx, temCancel := context.WithCancel(ctx)
				temCh, err := config.WatchStorageClusterConfig(
					templateCtx, cs, vdiskNBDConfig.TemplateStorageClusterID)
				if err != nil {
					temCancel()
					log.Errorf("failed to watch new template cluster config: %v", err)
					continue
				}
				templateClusterCh = temCh
				templateCancel()
				templateCancel = temCancel

			case templateClusterCfg = <-templateClusterCh:
				err = tsc.cluster.SetStorageconfig(templateClusterCfg)
				if err != nil {
					log.Errorf("failed to update new template cluster config: %v", err)
				}
			}
		}
	}()

	return nil
}

// mapErrorToBroadcastStatus maps the given error,
// returned by a `Connection` operation to a broadcast's message status.
func mapErrorToBroadcastStatus(err error) log.MessageStatus {
	if netErr, ok := err.(net.Error); ok {
		if netErr.Timeout() {
			return log.StatusServerTimeout
		}
		if netErr.Temporary() {
			return log.StatusServerTempError
		}
	} else if err == io.EOF {
		return log.StatusServerDisconnect
	}

	return log.StatusUnknownError
}
