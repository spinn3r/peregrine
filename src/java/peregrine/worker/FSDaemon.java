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
package peregrine.worker;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.logging.*;
import org.jboss.netty.bootstrap.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.*;

import peregrine.config.*;
import peregrine.controller.*;
import peregrine.rpc.*;
import peregrine.shuffle.receiver.*;
import peregrine.task.*;
import peregrine.util.*;
import peregrine.util.netty.*;

import com.spinn3r.log5j.Logger;

/**
 * Main daemon for handling filesystem operations.
 */
public class FSDaemon extends BaseDaemon {

    private static final Logger log = Logger.getLogger();
    
    /**
     * Each daemon can only have one shuffle instance.
     */
    public ShuffleReceiverFactory shuffleReceiverFactory;

    private HeartbeatTimer heartbeatTimer;

    public FSDaemon( Config config ) {

        setConfig( config );

        init();

        shuffleReceiverFactory = new ShuffleReceiverFactory( config ); 

        if ( config.getController() != null )
            heartbeatTimer = new HeartbeatTimer( config );
        
    }

    @Override
    public ChannelPipelineFactory getChannelPipelineFactory() {
        return new FSPipelineFactory( getConfig(), this );
    }
    
    public void shutdown() {

        if ( heartbeatTimer != null )
            heartbeatTimer.cancel();

        super.shutdown();
        
    }

}
