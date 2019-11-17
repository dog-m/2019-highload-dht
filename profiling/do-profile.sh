#!/bin/bash
# * wrk should be in /usr/local/bin (see [https://github.com/giltene/wrk2/wiki/Installing-wrk2-on-Linux])
# 1. 'sudo ~/pre-profile.sh' (or do manually)
# 2. '~/gradlew run' in separate console
# 3. '~/warm-up.sh'
# 4. '~/do-profile.sh' in async-profiler root(!) directory

### User-defined values ###

STAGE=6
TARGET='Cluster'  # Cluster on 4, Server on 1..3
OUTPUT_DIR="$HOME"
URL='http://localhost:8080'
WRK_THREADS=2
WRK_CONNECTIONS=10
WRK_REQUESTS_PER_SECOND=200
WRK_SCRIPTS_DIR="$HOME/2019-highload-dht/src/test/wrk"
PROFILING_TYPES=(cpu alloc lock)
HTTP_METHODS=(get put)  # script names(!)
# in seconds(!)
PROFILE_TIME_WRK=60
PROFILE_TIME_ASYNC_PROFILER=90
RELAX_TIME=2  # hardware isn't just pile of cheap junk

### (not quite) user-defined values ###

PREFIX="stage-$STAGE"
WRK_PARAMS="-t${WRK_THREADS} -c${WRK_CONNECTIONS} -R${WRK_REQUESTS_PER_SECOND}"

### *Magic* ###

echo 'Reminder: are you already executed PRE-PROFILE and WARM-UP scripts?'
echo "Stage prefix: $PREFIX"

WRK_TIME=$(( ${PROFILE_TIME_WRK} + ${RELAX_TIME} ))
ASYNC_PROFILER_TIME=$(( ${PROFILE_TIME_ASYNC_PROFILER} + ${RELAX_TIME} ))
DURATION=$(( ${#HTTP_METHODS} * (${WRK_TIME} + ${ASYNC_PROFILER_TIME} * ${#PROFILING_TYPES}) ))
DURATION=`date -d@${DURATION} -u +"%Mm %Ss"`

PID=`jps | grep ${TARGET} | cut -f 1 -d " "`

if [ "${PID}" == "" ]
then
  echo "Java process ${TARGET} not found"
  exit

else
  echo "Found process ${TARGET} on id ${PID}"
  echo "Have a cup of tea. It will take around ${DURATION}"

  echo 'Collecting WRK2 data...'
    for METHOD in "${HTTP_METHODS[@]}"
    do
      echo "[${METHOD}]"
      wrk ${WRK_PARAMS} -d${PROFILE_TIME_WRK}s -s ${WRK_SCRIPTS_DIR}/${METHOD}.lua --latency ${URL} > ${OUTPUT_DIR}/${PREFIX}_wrk_${METHOD}.log
      sleep ${RELAX_TIME}s
    done
  echo 'WRK2 done'

  echo 'Collecting ASYNC-PROFILER data...'
    for METHOD in "${HTTP_METHODS[@]}"
    do
      for TYPE in "${PROFILING_TYPES[@]}"
      do
        echo "[${METHOD}] ${TYPE}"
        ./profiler.sh -d ${PROFILE_TIME_ASYNC_PROFILER} -e ${TYPE} -f ${OUTPUT_DIR}/${PREFIX}_async-profiler_${METHOD}-${TYPE}.svg ${PID} &
        wrk ${WRK_PARAMS} -d${PROFILE_TIME_ASYNC_PROFILER}s -s ${WRK_SCRIPTS_DIR}/${METHOD}.lua ${URL} > /dev/null
        sleep ${RELAX_TIME}s
      done
    done
  echo 'ASYNC-PROFILER done'

  echo 'All done'
fi
