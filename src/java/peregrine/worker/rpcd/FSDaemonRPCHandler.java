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
package peregrine.worker.rpcd;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import peregrine.util.*;
import peregrine.worker.*;
import peregrine.worker.rpcd.delegate.*;
import peregrine.rpc.*;
import peregrine.rpcd.*;
import peregrine.rpcd.delegate.*;
import peregrine.controller.*;
import peregrine.controller.rpcd.*;
import peregrine.controller.rpcd.delegate.*;

import com.spinn3r.log5j.*;

/**
 */
public class FSDaemonRPCHandler extends BaseRPCHandler<FSDaemon> {

	private static Map<String,RPCDelegate<FSDaemon>> handlers = new HashMap() {{
    	
    	put( "/shuffler/RPC",    new ShufflerRPCDelegate() );
        put( "/map/RPC",         new MapperRPCDelegate() );
        put( "/reduce/RPC",      new ReducerRPCDelegate() );
        put( "/merge/RPC",       new MergerRPCDelegate() );
        
    }};

    private FSDaemon daemon;

    private FSHandler handler;

    public FSDaemonRPCHandler( FSDaemon daemon, FSHandler handler, String uri ) {
    	
        super( daemon.getExecutorService( ControllerRPCHandler.class ), uri );
    	
        this.handler = handler;
        this.daemon = daemon;
        
    }
    
    @Override
    public RPCDelegate<FSDaemon> getRPCDelegate( String path ) {
    	return handlers.get( path );	
    }
 
    @Override
    public void handleMessage( RPCDelegate handler, Channel channel, Message message ) 
        throws Exception {
        	
       	handler.handleMessage(  daemon, channel, message );
        	
    }   
    
}
