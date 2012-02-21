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
package peregrine.controller.rpcd;

import static org.jboss.netty.handler.codec.http.HttpMethod.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import java.net.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.frame.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.controller.rpcd.delegate.*;
import peregrine.http.*;
import peregrine.rpc.*;
import peregrine.rpcd.*;
import peregrine.rpcd.delegate.*;
import com.spinn3r.log5j.*;

/**
 */
public class ControllerRPCHandler extends BaseRPCHandler<ControllerDaemon> {

    private static final Logger log = Logger.getLogger();

    private static final ControllerRPCDelegate delegate = new ControllerRPCDelegate();

    protected Config config;
    protected ControllerDaemon controllerDaemon;

    public ControllerRPCHandler( Config config,
                                 ControllerDaemon controllerDaemon,
                                 String uri ) {

        super( controllerDaemon.getExecutorService( ControllerRPCHandler.class ), uri );

        this.config = config;
        this.controllerDaemon = controllerDaemon;

    }

    @Override
    public RPCDelegate<ControllerDaemon> getRPCDelegate( String uri ) {
        return delegate;
    }

    @Override
    public void handleMessage( RPCDelegate<ControllerDaemon> handler, Channel channel, Message message ) 
    	throws Exception {

    	handler.handleMessage( controllerDaemon, channel, message );
    	
    }

}

