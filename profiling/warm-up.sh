wrk -t2 -c10 -d60s -R1000 -s ~/2019-highload-dht/src/test/wrk/get.lua http://localhost:8080
wrk -t2 -c10 -d60s -R1000 -s ~/2019-highload-dht/src/test/wrk/put.lua http://localhost:8080
wrk -t2 -c10 -d30s -R1000 -s ~/2019-highload-dht/src/test/wrk/get.lua http://localhost:8080
