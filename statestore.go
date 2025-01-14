package kinesumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/guregu/dynamo/v2"
)

//go:generate mockgen -source=statestore.go -destination=statestore_mock.go -package=kinesumer StateStore

// Error codes.
var (
	ErrNoShardCache  = errors.New("kinesumer: shard cache not found")
	ErrEmptyShardIDs = errors.New("kinesumer: empty shard ids given")
)

type (
	// StateStore is a distributed key-value store for managing states.
	StateStore interface {
		GetShards(ctx context.Context, stream string) (Shards, error)
		UpdateShards(ctx context.Context, stream string, shards Shards) error
		ListAllAliveClientIDs(ctx context.Context) ([]string, error)
		RegisterClient(ctx context.Context, clientID string) error
		DeregisterClient(ctx context.Context, clientID string) error
		PingClientAliveness(ctx context.Context, clientID string) error
		PruneClients(ctx context.Context) error
		ListCheckPoints(ctx context.Context, stream string, shardIDs []string) (map[string]string, error)
		UpdateCheckPoints(ctx context.Context, checkpoints []*ShardCheckPoint) error
	}

	db struct {
		client *dynamo.DB
		table  *dynamo.Table
	}

	// stateStore implements the StateStore with AWS DynamoDB. (default)
	stateStore struct {
		app string
		db  *db
	}
)

// newStateStore initializes the state store.
func newStateStore(cfg *Config) (StateStore, error) {
	ctx := context.TODO()

	var client *dynamo.DB
	if cfg.DynamoClient != nil {
		client = dynamo.NewFromIface(cfg.DynamoClient)
	} else {
		awsCfg, err := config.LoadDefaultConfig(
			ctx,
			config.WithRegion(cfg.DynamoDBRegion),
			config.WithBaseEndpoint(cfg.DynamoDBEndpoint),
		)
		if err != nil {
			return nil, fmt.Errorf("kinesumer: failed to create an aws config: %w", err)
		}
		client = dynamo.New(awsCfg)
	}

	// Ping-like request to check if client can reach to DynamoDB.
	table := client.Table(cfg.DynamoDBTable)
	if _, err := table.Describe().Run(ctx); err != nil {
		return nil, fmt.Errorf("kinesumer: client can't access to dynamodb: %w", err)
	}
	return &stateStore{
		app: cfg.App,
		db: &db{
			client: client,
			table:  &table,
		},
	}, nil
}

// GetShards fetches a cached shard list.
func (s *stateStore) GetShards(
	ctx context.Context, stream string,
) (Shards, error) {
	var (
		key   = buildShardCacheKey(s.app)
		cache *stateShardCache
	)
	err := s.db.table.
		Get("pk", key).
		Range("sk", dynamo.Equal, stream).
		Consistent(true).
		One(ctx, &cache)
	if errors.Is(err, dynamo.ErrNotFound) {
		return nil, ErrNoShardCache
	} else if err != nil {
		return nil, err
	}
	return cache.Shards, nil
}

// UpdateShards updates a shard list cache.
func (s *stateStore) UpdateShards(
	ctx context.Context, stream string, shards Shards,
) error {
	key := buildShardCacheKey(s.app)
	err := s.db.table.
		Update("pk", key).
		Range("sk", stream).
		Set("shards", shards).
		Run(ctx)
	if err != nil {
		return err
	}
	return nil
}

// ListAllAliveClientIDs fetches an id list of all alive clients.
func (s *stateStore) ListAllAliveClientIDs(ctx context.Context) ([]string, error) {
	var (
		key     = buildClientKey(s.app)
		now     = time.Now()
		clients []*stateClient
	)
	err := s.db.table.
		Get("pk", key).
		Range("sk", dynamo.Greater, " ").
		Filter("last_update > ?", now.Add(-outdatedGap)).
		Order(dynamo.Ascending).
		All(ctx, &clients)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, client := range clients {
		ids = append(ids, client.ClientID)
	}
	return ids, nil
}

// RegisterClient registers a client to state store.
func (s *stateStore) RegisterClient(
	ctx context.Context, clientID string,
) error {
	var (
		key = buildClientKey(s.app)
		now = time.Now()
	)
	client := stateClient{
		ClientKey:  key,
		ClientID:   clientID,
		LastUpdate: now,
	}
	if err := s.db.table.Put(client).Run(ctx); err != nil {
		return err
	}
	return nil
}

// DeregisterClient de-registers a client from the state store.
func (s *stateStore) DeregisterClient(
	ctx context.Context, clientID string,
) error {
	key := buildClientKey(s.app)
	err := s.db.table.
		Delete("pk", key).
		Range("sk", clientID).
		Run(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (s *stateStore) PingClientAliveness(
	ctx context.Context, clientID string,
) error {
	var (
		key = buildClientKey(s.app)
		now = time.Now()
	)
	err := s.db.table.
		Update("pk", key).
		Range("sk", clientID).
		Set("last_update", now).
		Run(ctx)
	if err != nil {
		return err
	}
	return nil
}

// PruneClients prune clients that have been inactive for a certain amount of time.
func (s *stateStore) PruneClients(ctx context.Context) error {
	var (
		key = buildClientKey(s.app)
		now = time.Now()
	)
	var outdated []*stateClient
	err := s.db.table.
		Get("pk", key).
		Range("last_update", dynamo.Less, now.Add(-outdatedGap)).
		Index("index-client-key-last-update").
		All(ctx, &outdated)
	if err != nil {
		return err
	}

	if len(outdated) == 0 {
		return nil
	}

	var keys []dynamo.Keyed
	for _, client := range outdated {
		keys = append(
			keys, dynamo.Keys{client.ClientKey, client.ClientID},
		)
	}

	_, err = s.db.table.
		Batch("pk", "sk").
		Write().
		Delete(keys...).
		Run(ctx)
	if err != nil {
		return err
	}
	return nil
}

// ListCheckPoints fetches check point sequence numbers for multiple shards.
func (s *stateStore) ListCheckPoints(
	ctx context.Context, stream string, shardIDs []string,
) (map[string]string, error) {
	if len(shardIDs) == 0 {
		return nil, ErrEmptyShardIDs
	}

	var (
		keys   []dynamo.Keyed
		seqMap = make(map[string]string)
	)
	for _, id := range shardIDs {
		keys = append(
			keys,
			dynamo.Keys{buildCheckPointKey(s.app, stream), id},
		)
	}

	var checkPoints []*stateCheckPoint
	err := s.db.table.
		Batch("pk", "sk").
		Get(keys...).
		All(ctx, &checkPoints)
	if errors.Is(err, dynamo.ErrNotFound) {
		return seqMap, nil
	} else if err != nil {
		return nil, err
	}

	for _, checkPoint := range checkPoints {
		seqMap[checkPoint.ShardID] = checkPoint.SequenceNumber
	}
	return seqMap, nil
}

// UpdateCheckPoints updates the check point sequence numbers for multiple shards.
func (s *stateStore) UpdateCheckPoints(ctx context.Context, checkpoints []*ShardCheckPoint) error {
	stateCheckPoints := make([]interface{}, len(checkpoints))
	for i, checkpoint := range checkpoints {
		stateCheckPoints[i] = stateCheckPoint{
			StreamKey:      buildCheckPointKey(s.app, checkpoint.Stream),
			ShardID:        checkpoint.ShardID,
			SequenceNumber: checkpoint.SequenceNumber,
			LastUpdate:     checkpoint.UpdatedAt,
		}
	}

	// TODO(proost): check written bytes
	_, err := s.db.table.
		Batch("pk", "sk").
		Write().
		Put(stateCheckPoints...).
		Run(ctx)
	if err != nil {
		return err
	}
	return nil
}
