package storage

import (
	"context"

	"github.com/lunny/log"
	"github.com/zero-os/0-Disk/config"
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
		cluster: cluster,
		pool:    pool,
	}
	err = templateCluster.spawnConfigReloader(ctx, vdiskID, cs)
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
	cluster *ardb.Cluster
	pool    *ardb.Pool
	cancel  context.CancelFunc
}

// Do implements StorageCluster.Do
func (tsc *TemplateCluster) Do(action ardb.StorageAction) (reply interface{}, err error) {
	return tsc.cluster.Do(action)
}

// DoFor implements StorageCluster.DoFor
func (tsc *TemplateCluster) DoFor(objectIndex int64, action ardb.StorageAction) (reply interface{}, err error) {
	return tsc.cluster.DoFor(objectIndex, action)
}

// Close any open resources
func (tsc *TemplateCluster) Close() error {
	tsc.cancel()
	tsc.pool.Close()
	return nil
}

func (tsc *TemplateCluster) spawnConfigReloader(ctx context.Context, vdiskID string, cs config.Source) error {
	ctx, tsc.cancel = context.WithCancel(context.Background())

	vdiskNBDRefCh, err := config.WatchVdiskNBDConfig(ctx, cs, vdiskID)
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
