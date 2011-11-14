package peregrine.pfsd.rpc;

import java.util.*;

import org.jboss.netty.channel.*;

import peregrine.pfsd.*;

import peregrine.config.Host;
import peregrine.config.Partition;
import peregrine.rpc.*;

/**
 */
public class ControllerHandler extends RPCHandler {

	private Map<String,RPCHandler> handlers = new HashMap() {{
	
		put( "complete", new RPCHandler() {
		
		    public void handleMessage( Channel channel, FSDaemon daemon, Message message )
		            throws Exception {
		    	
	            Host host       = Host.parse( message.get( "host" ) );
	            Partition part  = new Partition( message.getInt( "partition" ) );

	            daemon.getScheduler().markComplete( host, part );

	            return;
		    	
		    }
		} );
		
		put( "failed", new RPCHandler() {
			
		    public void handleMessage( Channel channel, FSDaemon daemon, Message message )
		            throws Exception {

	            Host host          = Host.parse( message.get( "host" ) );
	            Partition part     = new Partition( message.getInt( "partition" ) );
	            String stacktrace  = message.get( "stacktrace" );

	            daemon.getScheduler().markFailed( host, part, stacktrace );
	            
	            return;
		    	
		    }
		} );		

		put( "progress", new RPCHandler() {
			
		    public void handleMessage( Channel channel, FSDaemon daemon, Message message )
		            throws Exception {
	            
	            return;
		    	
		    }
		} );	
		
		put( "heartbeat", new RPCHandler() {
			
		    public void handleMessage( Channel channel, FSDaemon daemon, Message message )
		            throws Exception {
	            		    	
	            Host host = Host.parse( message.get( "host" ) );

                // mark this host as online for the entire controller.
                daemon.getConfig().getMembership().getOnline().mark( host );
		    	
	            return;
		    	
		    }
		} );
		
		put( "gossip", new RPCHandler() {
			
		    public void handleMessage( Channel channel, FSDaemon daemon, Message message )
		            throws Exception {

		    	// mark that a machine has failed to process some unit of work.
		    	
	            return;
		    	
		    }
		} );
		
	}};
	
    public void handleMessage( Channel channel, FSDaemon daemon, Message message )
        throws Exception {

        String action = message.get( "action" );
        
        RPCHandler handler = handlers.get( action );
        
        if ( handler == null )
        	throw new Exception( String.format( "No handler for action %s with message %s", action, message ) );

        handler.handleMessage( channel, daemon, message );	
        
    }
    
}
