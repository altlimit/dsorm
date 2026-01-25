package ds

import (
	"context"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var (
	// Measures
	mCacheHit  = stats.Int64("cache_hit", "Cache hits", stats.UnitDimensionless)
	mCacheMiss = stats.Int64("cache_miss", "Cache misses", stats.UnitDimensionless)

	// Tag Keys
	KeyKind, _ = tag.NewKey("kind")

	// Views
	AllViews = []*view.View{
		{
			Name:        "dsorm/cache_hit",
			Description: "Cache hits",
			Measure:     mCacheHit,
			Aggregation: view.Sum(),
			TagKeys:     []tag.Key{KeyKind},
		},
		{
			Name:        "dsorm/cache_miss",
			Description: "Cache misses",
			Measure:     mCacheMiss,
			Aggregation: view.Sum(),
			TagKeys:     []tag.Key{KeyKind},
		},
	}
)

func cacheStatsByKind(ctx context.Context, items []cacheItem) error {
	cacheStats := make(map[string]*[2]int64)

	for _, item := range items {
		if _, ok := cacheStats[item.key.Kind]; !ok {
			cacheStats[item.key.Kind] = &[2]int64{0, 0}
		}
		switch item.state {
		case done: // Hit
			cacheStats[item.key.Kind][0]++
		default: // Miss
			cacheStats[item.key.Kind][1]++
		}

	}

	for key, s := range cacheStats {
		if err := stats.RecordWithTags(ctx,
			[]tag.Mutator{
				tag.Upsert(KeyKind, key),
			},
			mCacheHit.M(s[0]),
			mCacheMiss.M(s[1]),
		); err != nil {
			return err
		}
	}

	return nil
}
