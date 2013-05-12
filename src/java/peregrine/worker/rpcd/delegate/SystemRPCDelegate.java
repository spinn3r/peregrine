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
package peregrine.worker.rpcd.delegate;

import org.jboss.netty.channel.*;
import org.jboss.netty.buffer.*;

import peregrine.rpc.*;
import peregrine.rpcd.delegate.*;
import peregrine.worker.*;

import peregrine.os.*;

/**
 * Handles all shuffle related RPC messages.
 */
public class SystemRPCDelegate extends RPCDelegate<WorkerDaemon> {

    /**
     * Get status (stat) information on the current daemon.  This includes the
     * pid so that control daemons can connect to the port, get the pid, and
     * kill it if necessary.
     */
    @RPC
    public ChannelBuffer stat( WorkerDaemon daemon, Channel channel, Message message ) throws Exception {

        Message response = new Message();
        response.put( "pid", "" + unistd.getpid() );

        return ChannelBuffers.wrappedBuffer( response.toString().getBytes() );
        
    }

}
