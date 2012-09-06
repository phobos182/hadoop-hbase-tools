hadoop-hbase-tools
==================
Small collections of Ruby scripts that make running Hadoop / HBase clusters easier

Fair Scheduler
------
    root@server:/usr/share/diamond/user_scripts# ./fair_scheduler.rb -h
    Usage: scheduler_metrics -s [hadoop_jobtracker]
        -s, --server SERVER              Hadoop JobTracker
        -k, --prefix PREFIX              Graphite key prefix
        -h, --help                       Display this screen

Example output
----
    root@server:/usr/share/diamond/user_scripts# ./fair_scheduler.rb -s jobs-aa-hnn -p mykey
    fairscheduler.pool.pool1.jobs 4
    fairscheduler.pool.pool1.maps 3
    fairscheduler.pool.pool1.reduces 1
    fairscheduler.pool.pool2.jobs 0
    fairscheduler.pool.pool2.maps 0
    fairscheduler.pool.pool2.reduces 0
    fairscheduler.pool.pool3.jobs 0
    fairscheduler.pool.pool3.maps 0

