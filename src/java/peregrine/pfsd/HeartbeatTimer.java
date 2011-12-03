package peregrine.pfsd;

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

import com.spinn3r.log5j.Logger;
import peregrine.util.netty.*;

public class HeartbeatTimer extends Timer {

    private static final Logger log = Logger.getLogger();

    public static long ONLINE_SLEEP_INTERVAL  = 30000L;
    
    public static long OFFLINE_SLEEP_INTERVAL = 1000L;      

    private boolean cancelled = false;

    private Config config = null;
    
    /**
     *
     */
    public HeartbeatTimer( Config config ) {
        super( HeartbeatTimer.class.getName(), true );

        this.config = config;

        schedule( new HeartbeatTimerTask( this ) , 0 );
        
    }

    @Override
    public void cancel() {
        cancelled = true;
        super.cancel();
    }

    class HeartbeatTimerTask extends TimerTask {

        HeartbeatTimer timer;

        public HeartbeatTimerTask( HeartbeatTimer timer ) {
            this.timer = timer;
        }

        @Override
        public void run() {

            long delay;
            
            if ( config.getMembership().sendHeartbeatToController() ) {
                delay = ONLINE_SLEEP_INTERVAL;
            } else {
                delay = OFFLINE_SLEEP_INTERVAL;
            }
            
            if ( ! cancelled ) 
                timer.schedule( new HeartbeatTimerTask( timer ), delay );

        }

    }

}
