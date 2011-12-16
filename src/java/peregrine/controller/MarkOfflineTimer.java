package peregrine.controller;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.logging.*;
import org.jboss.netty.bootstrap.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.*;

import peregrine.config.*;
import peregrine.rpc.*;
import peregrine.shuffle.receiver.*;
import peregrine.task.*;
import peregrine.util.*;
import peregrine.pfsd.*;

import com.spinn3r.log5j.Logger;
import peregrine.util.netty.*;

/**
 * Timer for marking a host offline if we do not receive heartbeat messages in a
 * timely manner.
 */
public class MarkOfflineTimer extends Timer {

    private static final Logger log = Logger.getLogger();

    public static long FAILED_INTERVAL  = HeartbeatTimer.ONLINE_SLEEP_INTERVAL * 4L;

    /**
     * How often do we want to wake up and check for machines that have failed.
     */
    public static long SCHEDULE_DELAY = 30000L;

    private boolean cancelled = false;

    private Config config = null;

    private ClusterState clusterState;
    
    /**
     *
     */
    public MarkOfflineTimer( Config config,
                             ClusterState clusterState ) {
        
        super( MarkOfflineTimer.class.getName(), true );

        this.config = config;
        this.clusterState = clusterState;
        
        schedule( new MarkOfflineTimerTask( this ) , FAILED_INTERVAL );
        
    }

    @Override
    public void cancel() {
        cancelled = true;
        super.cancel();
    }

    class MarkOfflineTimerTask extends TimerTask {

        MarkOfflineTimer timer;

        public MarkOfflineTimerTask( MarkOfflineTimer timer ) {
            this.timer = timer;
        }

        @Override
        public void run() {

            long now = System.currentTimeMillis();
            
            for( Host host : config.getHosts() ) {

            	Online online = clusterState.getOnline();
            	
                if ( online.contains( host ) == false || now - online.get( host ) > FAILED_INTERVAL ) {

                	clusterState.getOffline().mark( host );
                    
                }
                
            }

            if ( ! cancelled ) 
                timer.schedule( new MarkOfflineTimerTask( timer ), SCHEDULE_DELAY );

        }

    }

}
