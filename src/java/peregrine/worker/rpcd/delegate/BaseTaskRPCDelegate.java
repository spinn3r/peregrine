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

import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;

import peregrine.config.*;
import peregrine.util.*;
import peregrine.worker.*;
import peregrine.rpc.*;
import peregrine.rpcd.delegate.*;
import peregrine.io.*;
import peregrine.task.*;

import com.spinn3r.log5j.*;

/**
 * Handles messages related to tasks.  Keeps track of them and resets them and
 * supports killing them as well.
 * 
 */
public class BaseTaskRPCDelegate extends RPCDelegate<FSDaemon> {

    private static final Logger log = Logger.getLogger();

    private ConcurrentHashMap<Work,Task> tasks = new ConcurrentHashMap();
    
    /**
     * Reset state between partition runs.
     */
    @RPC
    public ChannelBuffer reset( FSDaemon daemon, Channel channel, Message message )
        throws Exception {

        log.info( "Going to reset()" );
        
        tasks = new ConcurrentHashMap();

        return null;
        
    }

    /**
     * Kill a given task by partition.
     */
    @RPC
    public ChannelBuffer kill( FSDaemon daemon, Channel channel, Message message )
        throws Exception {

        Input input   = new Input( message.getList( "input" ) );
        Work work     = new Work( daemon.getConfig().getHost(), input, message.getList( "work" ) );

        log.info( "Killing task on %s", work );

        Task task = tasks.get( work );

        if ( task == null )
            return null;

        task.setKilled( true );

        log.info( "Task on %s sent kill request.", work );

        return null;
        
    }

    /**
     * Track a given task so it can be killed, etc.
     */
    protected void trackTask( Work work, Task task ) {
        tasks.put( work, task );
    }
    
}
