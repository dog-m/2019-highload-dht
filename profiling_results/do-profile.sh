# * wrk should be in /local/usr/bin (see wiki)
# 1. './pre-profile.sh'
# 2. './gradlew run' in separate console
# 3. './warm-up.sh'
# 4. './do-profile.sh' in async-profiler root directory

STAGE=4
TARGET='Cluster' # Cluster on 4, Server on 1..3

ID=`jps | grep $TARGET | cut -f 1 -d " "`

PREFIX="stage-$STAGE"

echo 'Reminder: are you already executed PRE-PROFILE and WARM-UP scripts?'
echo "Stage prefix: $PREFIX"

if [ "${ID}" == "" ]
then
  echo "Process $TARGET not found"
else
  echo "Found process $TARGET on id $ID"

  echo 'Collecting WRK2 data...'
    echo '[GET]'
    wrk -t2 -c10 -d60s -R1000 -s ~/2019-highload-dht/src/test/wrk/get.lua --latency http://localhost:8080 > ~/${PREFIX}_wrk_get.log
    echo '[PUT]'
    wrk -t2 -c10 -d60s -R1000 -s ~/2019-highload-dht/src/test/wrk/put.lua --latency http://localhost:8080 > ~/${PREFIX}_wrk_put.log
  echo 'WRK2 done'

  echo 'Collecting ASYNC-PROFILER data...'
    echo '[GET]'
    ./profiler.sh -d 90 -f ~/${PREFIX}_async-profiler_get.svg ${ID} &
    wrk -t2 -c10 -d90s -R1000 -s ~/2019-highload-dht/src/test/wrk/get.lua http://localhost:8080 > /dev/null
    echo '[PUT]'
    ./profiler.sh -d 90 -f ~/${PREFIX}_async-profiler_put.svg ${ID} &
    wrk -t2 -c10 -d90s -R1000 -s ~/2019-highload-dht/src/test/wrk/put.lua http://localhost:8080 > /dev/null

  echo 'All done'
fi
