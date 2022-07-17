### experimenting with sqlite and faktory, now go_system_test takes 4-5 seconds compared to 2-3 with redis.
### 1 queue 1 sqlite database
### 1 sorted set 1 sqlite database
### using modernc because of the overhead with the cgo calls... i expect improvements with go 1.18
### 1 max open conn for sqlite writings, wal mode activated, synchronous normal
### the main bottleneck is the sync around *sql.DB and the number of single writes