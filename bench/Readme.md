#### Results: 


#### Results after just adding the benchmark test on commit: 5f8bf8a3fec64e2df6d1032381a6084028fd7867 (Base line no changes)

BenchmarkClientServerRoundTripSmallMsg-12                  11257            105941 ns/op           17392 B/op        290 allocs/op
BenchmarkClientServerRoundTripBigStrMsg-12                  9408            127801 ns/op           71079 B/op        293 allocs/op
BenchmarkClientServerRoundTripBigMapBigStrMsg-12            3555            309087 ns/op          187275 B/op       2725 allocs/op

BenchmarkClientServerRoundTripSmallMsg-12                  10671            107704 ns/op           17393 B/op        290 allocs/op
BenchmarkClientServerRoundTripBigStrMsg-12                  8620            132313 ns/op           71168 B/op        293 allocs/op
BenchmarkClientServerRoundTripBigMapBigStrMsg-12            3576            315491 ns/op          187284 B/op       2725 allocs/op

BenchmarkClientServerRoundTripSmallMsg-12                  11174            110227 ns/op           17395 B/op        290 allocs/op
BenchmarkClientServerRoundTripBigStrMsg-12                  8658            139203 ns/op           71103 B/op        293 allocs/op
BenchmarkClientServerRoundTripBigMapBigStrMsg-12            3556            315230 ns/op          187242 B/op       2725 allocs/op

#### Results after just adding the benchmark test on commit: dad755d6cb11f4daab738eec8133554a7c3c48e7 (pre-compiled names regex)

*Just focusing on optimizing Allocations per Operation for this set of changes*

BenchmarkClientServerRoundTripSmallMsg-12                  11971             94264 ns/op           11850 B/op        208 allocs/op
BenchmarkClientServerRoundTripBigStrMsg-12                  9106            114902 ns/op           65520 B/op        211 allocs/op
BenchmarkClientServerRoundTripBigMapBigStrMsg-12            3831            298895 ns/op          181501 B/op       2642 allocs/op

BenchmarkClientServerRoundTripSmallMsg-12                  12870             92300 ns/op           11848 B/op        208 allocs/op
BenchmarkClientServerRoundTripBigStrMsg-12                  9277            114179 ns/op           65505 B/op        211 allocs/op
BenchmarkClientServerRoundTripBigMapBigStrMsg-12            3537            293994 ns/op          181645 B/op       2642 allocs/op

BenchmarkClientServerRoundTripSmallMsg-12                  12685             92157 ns/op           11848 B/op        208 allocs/op
BenchmarkClientServerRoundTripBigStrMsg-12                  9462            115722 ns/op           65519 B/op        211 allocs/op
BenchmarkClientServerRoundTripBigMapBigStrMsg-12            3644            295176 ns/op          181563 B/op       2642 allocs/op


