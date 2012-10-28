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

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;

import peregrine.rpc.*;
import peregrine.rpcd.delegate.*;
import peregrine.worker.*;

/**
 * Handles all shuffle related RPC messages.
 *
 * <h2>Protocol interaction</h2>
 * <p>
 * <img src="https://bitbucket.org/burtonator/peregrine/raw/78abaa786a1b650601eed017bff573f968bf403f/misc/shuffle-flush.png" border="1"/>
 */
public class ShufflerRPCDelegate extends RPCDelegate<FSDaemon> {

    /**
     * Flush pending shuffle data to disk.  This is called at the end of jobs to
     * verify that we don't have pending shuffle data in memory.
     */
    @RPC
    public ChannelBuffer flush( FSDaemon daemon, Channel channel, Message message ) throws IOException {
        // FIXME: this should be async should it not?
        daemon.shuffleReceiverFactory.flush();
        return null;
    }

    /**
     * Delete all the shuffle data on disk for the shuffle with the given name.
     */
    @RPC
    public ChannelBuffer delete( FSDaemon daemon, Channel channel, Message message ) throws IOException {
        daemon.shuffleReceiverFactory.purge( message.get( "name" ) );
        return null;
    }

    /**
     * Purge all shuffle data in the shuffle directory.  This is done BEFORE
     * batch jobs to verify that we don't have any shuffle data (incorrectly)
     * sitting around from the previous batch job.  Shuffle data may be used
     * <b>within</b> a Batch but not between batches.  
     */
    @RPC
    public ChannelBuffer purge( FSDaemon daemon, Channel channel, Message message ) throws IOException {

        Files.purge( config.getShuffleDir() );
        return null;
        
    }
    
}
