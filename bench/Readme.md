










#### Results: 


#### Results after just adding the benchmark test on commit: 5f8bf8a3fec64e2df6d1032381a6084028fd7867

Started embedded etcd on url: http://localhost:21692goos: darwin
goarch: amd64
pkg: github.com/lytics/grid/bench
BenchmarkClientServerRoundTripSmallMsg-12                  11257            105941 ns/op           17392 B/op        290 allocs/op
BenchmarkClientServerRoundTripBigStrMsg-12                  9408            127801 ns/op           71079 B/op        293 allocs/op
BenchmarkClientServerRoundTripBigMapBigStrMsg-12            3555            309087 ns/op          187275 B/op       2725 allocs/op
PASS
ok      github.com/lytics/grid/bench    8.211s
Started embedded etcd on url: http://localhost:11899goos: darwin
goarch: amd64
pkg: github.com/lytics/grid/bench
BenchmarkClientServerRoundTripSmallMsg-12                  10671            107704 ns/op           17393 B/op        290 allocs/op
BenchmarkClientServerRoundTripBigStrMsg-12                  8620            132313 ns/op           71168 B/op        293 allocs/op
BenchmarkClientServerRoundTripBigMapBigStrMsg-12            3576            315491 ns/op          187284 B/op       2725 allocs/op
PASS
ok      github.com/lytics/grid/bench    7.180s
Started embedded etcd on url: http://localhost:3762goos: darwin
goarch: amd64
pkg: github.com/lytics/grid/bench
BenchmarkClientServerRoundTripSmallMsg-12                  11174            110227 ns/op           17395 B/op        290 allocs/op
BenchmarkClientServerRoundTripBigStrMsg-12                  8658            139203 ns/op           71103 B/op        293 allocs/op
BenchmarkClientServerRoundTripBigMapBigStrMsg-12            3556            315230 ns/op          187242 B/op       2725 allocs/op
PASS
ok      github.com/lytics/grid/bench    7.943s



