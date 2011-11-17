#!/bin/sh

BRANCH=burton-bench
DIR=/root/peregrine
LOGDIR=/var/log
MAX_MEMORY=256M
HOSTNAME=$(hostname)

start() {

    for host in `cat conf/peregrine.hosts`; do

        hostname=$(echo $host|grep -Eo '^[^:]+')

        if [ "$HOSTNAME" = "$hostname" ]; then
            echo $host
            ./bin/pfsd -h=$host > $LOGDIR/peregrine-$host.log 2> $LOGDIR/peregrine-$host.err &
        fi

    done

}

start
