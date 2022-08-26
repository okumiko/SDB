# SDB

- 一个简单的**单机**存储引擎，使用**bitcask**存储架构
- 支持string、list、hash、set、zset五种数据结构
- 自带文件压缩、内存统计等功能
- 旨在学习

## benchmark

### string set
goos: darwin
goarch: amd64
pkg: sdb/benchmark
cpu: Intel(R) Core(TM) i7-8700B CPU @ 3.20GHz
BenchmarkRoseDB_Set
BenchmarkRoseDB_Set-12    	  171963	      7596 ns/op	     780 B/op	      10 allocs/op