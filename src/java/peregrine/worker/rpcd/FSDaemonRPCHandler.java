/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package peregrine.worker.rpcd;

import java.util.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;

import peregrine.worker.*;
import peregrine.worker.rpcd.delegate.*;
import peregrine.rpc.*;
import peregrine.rpcd.*;
import peregrine.rpcd.delegate.*;
import peregrine.controller.rpcd.*;

/**
 * RPC handler for worker filesystem operation.
 */
public class FSDaemonRPCHandler extends BaseRPCHandler<WorkerDaemon> {

	private static Map<String,RPCDelegate<WorkerDaemon>> handlers = new HashMap() {{
    	
    	put( "/shuffler/RPC",    new ShufflerRPCDelegate() );
        put( "/map/RPC",         new MapperRPCDelegate() );
        put( "/reduce/RPC",      new ReducerRPCDelegate() );
        put( "/merge/RPC",       new MergerRPCDelegate() );
        put( "/system/RPC",      new SystemRPCDelegate() );
        
    }};

    private WorkerDaemon daemon;

    private WorkerHandler handler;

    public FSDaemonRPCHandler( WorkerDaemon daemon, WorkerHandler handler, String uri ) {
    	
        super( daemon.getExecutorService( ControllerRPCHandler.class ), uri );
    	
        this.handler = handler;
        this.daemon = daemon;
        
    }
    
    @Override
    public RPCDelegate<WorkerDaemon> getRPCDelegate( String path ) {
    	return handlers.get( path );	
    }
 
    @Override
    public ChannelBuffer handleMessage( RPCDelegate handler, Channel channel, Message message ) 
        throws Exception {
        	
       	return handler.handleMessage(  daemon, channel, message );
        
    }   
    
}
