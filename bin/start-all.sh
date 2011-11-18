#!/bin/sh

export BRANCH=burton-bench
export DIR=/root/peregrine
export LOGDIR=/var/log
export MAX_MEMORY=256M
export MAX_DIRECT_MEMORY=1000M
export HOSTNAME=$(hostname)

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
