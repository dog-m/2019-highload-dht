## stage-3_wrk_get
wrk -t2 -c10 -d60s -R1000 -s ~/2019-highload-dht/src/test/wrk/get.lua --latency http://localhost:8080 > ~/stage-3_wrk_get.log

## stage-3_wrk_put
wrk -t2 -c10 -d60s -R1000 -s ~/2019-highload-dht/src/test/wrk/put.lua --latency http://localhost:8080 > ~/stage-3_wrk_put.log



## async-profiler
sudo su
echo 0 > /proc/sys/kernel/kptr_restrict
echo 1 > /proc/sys/kernel/perf_event_paranoid
exit


## stage-3_async-profiler_get
jps (см. номер у Server)
./profiler.sh -d 90 -f ~/stage-3_async-profiler_get.svg 0000 &
wrk -t2 -c10 -d90s -R1000 -s ~/2019-highload-dht/src/test/wrk/get.lua http://localhost:8080

## stage-3_async-profiler_put
jps (см. номер у Server)
./profiler.sh -d 90 -f ~/stage-3_async-profiler_put.svg 0000 &
wrk -t2 -c10 -d90s -R1000 -s ~/2019-highload-dht/src/test/wrk/put.lua http://localhost:8080
