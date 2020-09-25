#### Results: 

#### Base line after adding 4MB string message and with concurrent clients and server


BenchmarkClientServerRoundTripSmallMsg-12           	    4454	    260171 ns/op	  182423 B/op	    3049 allocs/op
BenchmarkClientServerRoundTripBigStrMsg-12          	    2883	    402069 ns/op	 1147026 B/op	    3110 allocs/op
BenchmarkClientServerRoundTripBigMapBigStrMsg-12    	    1014	   1149191 ns/op	 2930808 B/op	   42060 allocs/op
BenchmarkClientServerRoundTrip4MBStrMsg-12          	      70	  16805161 ns/op	93151828 B/op	    4344 allocs/op

BenchmarkClientServerRoundTripSmallMsg-12           	    4381	    266530 ns/op	  182443 B/op	    3049 allocs/op
BenchmarkClientServerRoundTripBigStrMsg-12          	    2923	    409588 ns/op	 1147743 B/op	    3111 allocs/op
BenchmarkClientServerRoundTripBigMapBigStrMsg-12    	    1010	   1186069 ns/op	 2931204 B/op	   42057 allocs/op
BenchmarkClientServerRoundTrip4MBStrMsg-12          	      68	  16603466 ns/op	93327727 B/op	    4060 allocs/op

BenchmarkClientServerRoundTripSmallMsg-12           	    4447	    265635 ns/op	  182437 B/op	    3049 allocs/op
BenchmarkClientServerRoundTripBigStrMsg-12          	    2875	    407557 ns/op	 1147588 B/op	    3111 allocs/op
BenchmarkClientServerRoundTripBigMapBigStrMsg-12    	    1020	   1148515 ns/op	 2929783 B/op	   42058 allocs/op
BenchmarkClientServerRoundTrip4MBStrMsg-12          	      68	  16214082 ns/op	92550489 B/op	    4153 allocs/op


#### Base Codec Marshal/UnMarshal benchmarks 

BenchmarkCodecMarshalBigMapBigStrMsg-12      	   25450	     45837 ns/op	   26643 B/op	     807 allocs/op
BenchmarkCodecUnmarshalBigMapBigStrMsg-12    	   35413	     33865 ns/op	   21602 B/op	     416 allocs/op

BenchmarkCodecMarshalBigMapBigStrMsg-12      	   25371	     45407 ns/op	   26643 B/op	     807 allocs/op
BenchmarkCodecUnmarshalBigMapBigStrMsg-12    	   34939	     33620 ns/op	   21604 B/op	     416 allocs/op

BenchmarkCodecMarshalBigMapBigStrMsg-12      	   24622	     46919 ns/op	   26643 B/op	     807 allocs/op
BenchmarkCodecUnmarshalBigMapBigStrMsg-12    	   34801	     34443 ns/op	   21601 B/op	     416 allocs/op

