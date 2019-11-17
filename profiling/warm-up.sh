WRK_REQUESTS_PER_SECOND=250

echo 1
wrk -t2 -c10 -d60s -R${WRK_REQUESTS_PER_SECOND} -s ~/2019-highload-dht/src/test/wrk/get.lua http://localhost:8080 > /dev/null

echo 2
wrk -t2 -c10 -d60s -R${WRK_REQUESTS_PER_SECOND} -s ~/2019-highload-dht/src/test/wrk/put.lua http://localhost:8080 > /dev/null

echo 3
wrk -t2 -c10 -d30s -R${WRK_REQUESTS_PER_SECOND} -s ~/2019-highload-dht/src/test/wrk/get.lua http://localhost:8080 > /dev/null
