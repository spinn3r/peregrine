package peregrine.controller.rpcd.delegate;

import java.util.*;

import org.jboss.netty.channel.*;

import peregrine.config.Host;
import peregrine.config.Partition;
import peregrine.controller.*;
import peregrine.rpc.*;
import peregrine.rpcd.delegate.*;

/**
 */
public class ControllerRPCDelegate extends RPCDelegate<ControllerDaemon> {

	private static Map<String,RPCDelegate> handlers = new HashMap() {{
	
		put( "complete", new RPCDelegate<ControllerDaemon>() {
		
		    public void handleMessage( ControllerDaemon controllerDaemon, Channel channel, Message message )
		            throws Exception {
		    			    	
	            Host host       = Host.parse( message.get( "host" ) );
	            Partition part  = new Partition( message.getInt( "partition" ) );

	            controllerDaemon.getScheduler().markComplete( host, part );

	            return;
		    	
		    }
		} );
		
		put( "failed", new RPCDelegate<ControllerDaemon>() {
			
		    public void handleMessage( ControllerDaemon controllerDaemon, Channel channel, Message message )
		            throws Exception {
		    	
	            Host host          = Host.parse( message.get( "host" ) );
	            Partition part     = new Partition( message.getInt( "partition" ) );
	            String stacktrace  = message.get( "stacktrace" );

	            controllerDaemon.getScheduler().markFailed( host, part, stacktrace );
	            
	            return;
		    	
		    }
		} );		

		put( "progress", new RPCDelegate<ControllerDaemon>() {
			
		    public void handleMessage( ControllerDaemon controllerDaemon, Channel channel, Message message )
		            throws Exception {
	            
	            return;
		    	
		    }
		} );	
		
		put( "heartbeat", new RPCDelegate<ControllerDaemon>() {
			
		    public void handleMessage( ControllerDaemon controllerDaemon, Channel channel, Message message )
		            throws Exception {
	            		    			    	
	            Host host = Host.parse( message.get( "host" ) );

                // mark this host as online for the entire controller.
	            controllerDaemon.getConfig().getMembership().getOnline().mark( host );
		    	
	            return;
		    	
		    }
		} );
		
		put( "gossip", new RPCDelegate<ControllerDaemon>() {
			
		    public void handleMessage( ControllerDaemon controllerDaemon, Channel channel, Message message )
		            throws Exception {
		    	
		    	// mark that a machine has failed to process some unit of work.

	            Host reporter = Host.parse( message.get( "reporter" ) );
	            Host failed   = Host.parse( message.get( "failed" ) );

	            controllerDaemon.getConfig().getMembership().getGossip().mark( reporter, failed ); 
                
	            return;
		    	
		    }
		} );
		
	}};
	
    public void handleMessage( ControllerDaemon controllerDaemon, Channel channel, Message message )
        throws Exception {
    	
        String action = message.get( "action" );
        
        RPCDelegate handler = handlers.get( action );
        
        if ( handler == null )
        	throw new Exception( String.format( "No handler for action %s with message %s", action, message ) );

        handler.handleMessage( controllerDaemon, channel, message );	
        
    }
    
}
