#!/usr/bin/ksh
###################################################################
# SCRIPT: killConsumer.sh
# PURPOSE: Kills the currently running consumer process
# WRITTEN BY: Abe Voelker
###################################################################

umask 000

TERM=ansi
PIDFILE=/tmp/Consumer.pid

if [ -f $PIDFILE ]; then #PID file exists
  PID=`cat $PIDFILE`
  if [ -n $PID ]; then #PID not null
    echo "PID is $PID"
    if [ -n "`ps -p $PID | grep $PID`" ]; then #Process exists with this PID
      echo "PID $PID is still running.  Now killing it..."
      kill $PID
    else
      echo "PID $PID is not running. Kill not necessary"
    fi
  else
    echo "PID file is empty!"
  fi
  rm $PIDFILE
else
  echo "PID file $PIDFILE doesn't exist!"
fi
