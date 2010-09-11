#!/usr/bin/ksh
###################################################################
# SCRIPT: startConsumer.sh
# PURPOSE: Starts a consumer process in the background
# WRITTEN BY: Abe Voelker
###################################################################

umask 000

TERM=ansi
LOGFILE=/tmp/Consumer.sh.log
PIDFILE=/tmp/Consumer.pid

#Make sure consumer is not already running
if [ -f $PIDFILE ]; then #PID file exists
  PID=`cat $PIDFILE`
  if [ -n $PID ]; then #PID not null
    if [ -n "`ps -p $PID | grep $PID`" ]; then #Process exists with this PID
      echo "ERROR - There is already a Consumer running! You must kill it before running this script." 1>&2
      exit 1
    fi
  fi
fi

#Start consumer process in the background
cd /usr/pro/cwh
mpro -b -p Stomp/examples/consumer/ConsumerStub.p >> $LOGFILE 2>&1 &

#Save the spawned consumer's process ID to file
echo $! > $PIDFILE
