#!/bin/sh

BRANCH=burton-bench
DIR=/root/peregrine
LOGDIR=/var/log
MAX_MEMORY=256M

start() {

    for host in `cat conf/peregrine.hosts`; do

        hostname=$(echo $host|grep -Eo '^[^:]+')

        if [ "$HOSTNAME" = "$hostname" ]; then

            echo $host

            cd $DIR
            export MAX_MEMORY=256M 
            export HOSTNAME 
            set -x
            ./bin/jexec peregrine.pfsd.Main -h=$host > $LOGDIR/peregrine-$host.log 2> $LOGDIR/peregrine-$host.err &
            set +x

        fi

    done

}

start
