echo 'Setting-up kernel attributes for ASYNC-PROFILER...'
  echo 0 > /proc/sys/kernel/kptr_restrict
  echo 1 > /proc/sys/kernel/perf_event_paranoid
echo 'Done'
