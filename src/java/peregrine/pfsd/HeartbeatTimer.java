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
import peregrine.pfsd.rpc.*;
import peregrine.rpc.*;
import peregrine.shuffle.receiver.*;
import peregrine.task.*;
import peregrine.util.*;

import com.spinn3r.log5j.Logger;
import peregrine.util.netty.*;

public class HeartbeatTimer extends Timer {
	
    public static final long ONLINE_SLEEP_INTERVAL  = 30000L;
    
    public static final long OFFLINE_SLEEP_INTERVAL = 1000L;    	

    private boolean cancelled = false;
    
    /**
     *
     */
    public HeartbeatTimer() {
        super( HeartbeatTimer.class.getName(), true );
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
	        	
				new Client().invoke( controller, "controller", message );
				
				return true;

			} catch ( IOException e ) {

                if ( Thread.currentThread().isInterrupted() )
                    return false;
                
				log.warn( String.format( "Unable to send heartbeat to %s: %s", 
                                         controller, e.getMessage() ) );
				
				return false;
			}
	   
	    }        	

    }

}
