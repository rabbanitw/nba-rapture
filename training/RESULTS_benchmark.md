# System comparison: ours vs Neil Paine's Estimated RAPTOR

Accuracy is in RESULTS_leaderboards_*.md. This is what the two cost to build,
store and run. They are different kinds of object: Paine's is a published
linear formula over 12 inputs; ours is 1,149 boosted trees over 1,140 features
blended with a ridge regression.

## Size

| | ours | Paine | ratio |
|---|---:|---:|---:|
| input features | 1,140 | 12 | 95x |
| parameters | 17,235 leaves + 1,141 ridge | 26 | 707x |
| serialized | 1.82 MB | 495 B | 3,675x |

## Training

| | ours | Paine |
|---|---:|---:|
| wall time | 19.1s LightGBM + 6.6s ridge | 0s (published constants) |
| peak allocation | 6 MB | 0 |

## Inference

| | ours | Paine | ratio |
|---|---:|---:|---:|
| batch of 494 (one season) | 3.97 ms | 0.021 ms | 193x |
| single row | 348 us | 14.8 us | 23x |
| throughput | 234,904 rows/s | 94,876,660 rows/s | 404x in Paine's favour |
| peak alloc, one batch | 9 KB | 16 KB | |
| input matrix resident | 2.25 MB | 0.047 MB | 48x |

## The cost that dominates: getting the features

Everything above is microseconds against a data pipeline measured in hours.
Per season, from this project's actual scrape logs:

| | ours | Paine |
|---|---|---|
| pbp box score | 62 API calls | 62 API calls |
| wowy on/off | ~1,560 calls, ~2h | ~1,560 calls, ~2h |
| player tracking | 28 calls, **needs a residential IP** | not needed |
| total | ~1,650 calls | ~1,622 calls |

Both need the same two expensive feeds, because Paine's OnCourt and OnOff terms
come from the same wowy scrape ours does. The real operational difference is the
14 tracking tables: only 28 requests, but stats.nba.com refuses datacenter IPs,
so that one feed forces a residential connection into the pipeline. Paine's model
has no such dependency.