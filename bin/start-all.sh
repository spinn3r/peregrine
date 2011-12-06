#!/bin/sh

export BRANCH=burton-bench
export DIR=/root/peregrine
export LOGDIR=/var/log
export MAX_MEMORY=256M
export MAX_DIRECT_MEMORY=700M
export HOSTNAME=$(hostname)

start() {

    for host in `cat conf/peregrine.hosts`; do

        hostname=$(echo $host|grep -Eo '^[^:]+')
        port=$(echo $host|grep -Eo '[^:]+$')

        basedir=/d0

        case $port in

            11114)
                basedir=/d1
                ;;
            11115)
                basedir=/d1
                ;;
            11116)
                basedir=/d2
                ;;
            11117)
                basedir=/d2
                ;;
            11118)
                basedir=/d3
                ;;
            11119)
                basedir=/d3
                ;;

        esac

        # Right now this is specific to our application and I'll need to fix it
        # later.

        if [ "$HOSTNAME" = "$hostname" ]; then
            echo $host $basedir
            ./bin/pfsd --host=$host --basedir=$basedir > $LOGDIR/peregrine-$host.log 2> $LOGDIR/peregrine-$host.err &
        fi

    done

}

start
