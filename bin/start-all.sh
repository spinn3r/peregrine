#!/bin/sh

BRANCH=burton-bench
DIR=/root/peregrine
LOGDIR=/var/log
MAX_MEMORY=256M

start() {

    for host in `cat conf/peregrine.hosts`; do

        echo $host

        hostname=$(echo $host|grep -Eo '^[^:]+')

        echo $hostname

        cd $DIR
        export MAX_MEMORY=256M 
        export HOSTNAME 
        ./bin/jexec peregrine.pfsd.Main -h=$host > $LOGDIR/peregrine-$host.log 2> $LOGDIR/peregrine-$host.err &

    done

}

start