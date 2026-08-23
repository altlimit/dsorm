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

	mLocalCacheHit   = stats.Int64("local_cache_hit", "Local query cache hits", stats.UnitDimensionless)
	mLocalCacheMiss  = stats.Int64("local_cache_miss", "Local query cache misses", stats.UnitDimensionless)
	mLocalCacheEvict = stats.Int64("local_cache_eviction", "Local query cache evictions", stats.UnitDimensionless)

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
		{
			Name:        "dsorm/local_cache_hit",
			Description: "Local query cache hits",
			Measure:     mLocalCacheHit,
			Aggregation: view.Sum(),
			TagKeys:     []tag.Key{KeyKind},
		},
		{
			Name:        "dsorm/local_cache_miss",
			Description: "Local query cache misses",
			Measure:     mLocalCacheMiss,
			Aggregation: view.Sum(),
			TagKeys:     []tag.Key{KeyKind},
		},
		{
			Name:        "dsorm/local_cache_eviction",
			Description: "Local query cache evictions",
			Measure:     mLocalCacheEvict,
			Aggregation: view.Sum(),
			TagKeys:     []tag.Key{KeyKind},
		},
	}
)

// RecordLocalCacheStats records local query cache activity for a kind. The
// measures live here so that registering [AllViews] covers every dsorm cache
// layer; the cache itself is implemented in the parent package.
func RecordLocalCacheStats(ctx context.Context, kind string, hits, misses, evictions int64) error {
	return stats.RecordWithTags(ctx,
		[]tag.Mutator{
			tag.Upsert(KeyKind, kind),
		},
		mLocalCacheHit.M(hits),
		mLocalCacheMiss.M(misses),
		mLocalCacheEvict.M(evictions),
	)
}

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
