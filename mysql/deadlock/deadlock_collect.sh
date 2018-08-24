#!/bin/bash

# deadlock db config
HOSTLOG=sr-mysql-03.gz.cn
PORTLOG=3308
USERLOG=pt_tools
PSWDLOG=xxxx

# 3306 master
HOST3306=sr-test-mysql-master-1.gz.cn
PORT3306=3306
USER3306=root
PSWD3306=xxxx
DB3306=pt_tools
TB3306=deadlock_3306

# 3307 master
HOST3307=sr-test-mysql-master-2.gz.cn
PORT3307=3307
USER3307=root
PSWD3307=xxxx
DB3307=pt_tools
TB3307=deadlock_3307

logdir=/usr/local/mysql/log

if [ ! -d $logdir ]; then
	mkdir -p logdir
fi

function startdeadlogger()
{
if [ $# != 6 ];then
	echo "parameters: $#, expected 6"
	exit 1
fi
HOST=$1
PORT=$2
USER=$3
PSWD=$4
DB=$5
TB=$6
pt-deadlock-logger --daemonize --log=$logdir/pt-deadlock-logger-${HOST}-${PORT}.log --create-dest-table --host=$HOST --port=$PORT --user=$USER --password=$PSWD --dest=h=$HOSTLOG,P=$PORTLOG,u=$USERLOG,p=$PSWDLOG,D=$DB,t=$DB
}

startdeadlogger $HOST3306 $PORT3306 $USER3306 $PSWD3306 $DB3306 $TB3306
startdeadlogger $HOST3307 $PORT3307 $USER3307 $PSWD3307 $DB3307 $TB3307
