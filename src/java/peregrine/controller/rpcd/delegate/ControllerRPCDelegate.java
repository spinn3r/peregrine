/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
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

                // FIXME verify that the config_checksum is correct...

                if ( ! controllerDaemon.getConfig().getChecksum().equals( message.get( "config_checksum" ) ) )
                    throw new Exception( "Config checksum from %s is invalid: " + host );
                
                // mark this host as online for the entire controller.
	            controllerDaemon.getClusterState().getOnline().mark( host );
		    	
	            return;
		    	
		    }
		} );
		
		put( "gossip", new RPCDelegate<ControllerDaemon>() {
			
		    public void handleMessage( ControllerDaemon controllerDaemon, Channel channel, Message message )
		            throws Exception {
		    	
		    	// mark that a machine has failed to process some unit of work.

	            Host reporter = Host.parse( message.get( "reporter" ) );
	            Host failed   = Host.parse( message.get( "failed" ) );

	            controllerDaemon.getClusterState().getGossip().mark( reporter, failed ); 
                
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
