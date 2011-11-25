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

    public static final long ONLINE_SLEEP_INTERVAL  = 30000L;
    
    public static final long OFFLINE_SLEEP_INTERVAL = 1000L;    	

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
            
	        	if ( sendHeartbeatToController() ) {
                delay = ONLINE_SLEEP_INTERVAL;
	        	} else {
                delay = OFFLINE_SLEEP_INTERVAL;
	        	}

            if ( ! cancelled ) 
                timer.schedule( new HeartbeatTimerTask( timer ), delay );

		}

	    public boolean sendHeartbeatToController() {
	    	
	        Message message = new Message();
	        message.put( "action", "heartbeat" );
	        message.put( "host",    config.getHost().toString() );
	        
	        Host controller = config.getController();
	        
	        try {        	
	        	
				new Client( true ).invoke( controller, "controller", message );
				
				return true;

			} catch ( IOException e ) {

                if ( Thread.currentThread().isInterrupted() )
                    return false;

                // we don't normally need to send this message because we would be overly verbose.
				log.debug( String.format( "Unable to send heartbeat to %s: %s", controller, e.getMessage() ) );
				
				return false;
			}
	   
	    }        	

    }

}
