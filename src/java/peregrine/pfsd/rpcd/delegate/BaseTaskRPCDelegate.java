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
package peregrine.pfsd.rpcd.delegate;

import peregrine.pfsd.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.channel.*;

import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.util.*;

import com.spinn3r.log5j.*;

import peregrine.rpc.*;
import peregrine.rpcd.delegate.*;

import peregrine.io.*;
import peregrine.task.*;

/**
 * Handles messages related to tasks.  Keeps track of them and resets them and
 * supports killing them as well.
 * 
 */
public class BaseTaskRPCDelegate extends RPCDelegate<FSDaemon> {

    private static final Logger log = Logger.getLogger();

    private ConcurrentHashMap<Partition,Task> tasks = new ConcurrentHashMap();
    
    /**
     * RPC call - reset between partition runs.
     */
    public void reset( FSDaemon daemon, Channel channel, Message message )
        throws Exception {

    }

    /**
     * RPC call - kill a given task by partition.
     */
    public void kill( FSDaemon daemon, Channel channel, Message message )
        throws Exception {

    }

    /**
     * Track a given task so it can be killed, etc.
     */
    protected void trackTask( Partition part, Task task ) {
        tasks.put( part, task );
    }
    
}
