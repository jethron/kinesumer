package kinesumer

import (
	"context"
	"slices"
	"time"
)

const (
	outdatedGap = 10 * time.Second
)

func (k *Kinesumer) doLeadershipSyncShardIDs(ctx context.Context) error {
	for _, stream := range k.streams {
		shards, err := k.listShards(stream)
		if err != nil {
			return err
		}
		if slices.Equal(k.shardCaches[stream], shards.ids()) {
			return nil
		}
		if err := k.stateStore.UpdateShards(ctx, stream, shards); err != nil {
			return err
		}
	}
	return nil
}

func (k *Kinesumer) doLeadershipPruneClients(ctx context.Context) error {
	if err := k.stateStore.PruneClients(ctx); err != nil {
		return err
	}
	return nil
}
