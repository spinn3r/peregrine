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

import java.io.*;

import org.jboss.netty.channel.*;

import peregrine.rpc.*;
import peregrine.rpcd.delegate.*;
import peregrine.worker.*;

/**
 * Handles all shuffle related RPC messages.
 */
public class ShufflerRPCDelegate extends RPCDelegate<FSDaemon> {

    /**
     * Flush pending shuffle data to disk.
     */
    @RPC
    public void flush( FSDaemon daemon, Channel channel, Message message ) throws IOException {
        // FIXME: this should be async should it not?
        daemon.shuffleReceiverFactory.flush();
    }

    /**
     * Purge/delete all the shuffle data on disk or the shuffle with the given
     * name.
     */
    @RPC
    public void purge( FSDaemon daemon, Channel channel, Message message ) throws IOException {
        daemon.shuffleReceiverFactory.purge( message.get( "name" ) );
    }

}
