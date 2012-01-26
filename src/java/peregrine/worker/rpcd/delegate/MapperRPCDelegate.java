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

import org.jboss.netty.channel.*;

import peregrine.config.*;
import peregrine.util.*;
import peregrine.worker.*;

import com.spinn3r.log5j.*;

import peregrine.rpc.*;
import peregrine.rpcd.delegate.*;

import peregrine.io.*;
import peregrine.task.*;

/**
 */
public class MapperRPCDelegate extends BaseTaskRPCDelegate {

    private static final Logger log = Logger.getLogger();

    /**
     * Execute a job on a given partition.
     */
    @RPC
    public void exec( FSDaemon daemon, Channel channel, Message message )
        throws Exception {

        log.info( "Going to map from action: %s", message );

        Input input            = readInput( message );
        Output output          = readOutput( message );
        Work work              = readWork( input, message );
        Class delegate         = Class.forName( message.get( "delegate" ) );
        Config config          = daemon.getConfig();

        log.info( "Running %s with input %s and output %s and work %s", delegate.getName(), input, output, work );

        exec( daemon, delegate, config, work, input, output );
        
        return;

    }

    protected void exec( FSDaemon daemon,
                         Class delegate,
                         Config config,
                         Work work,
                         Input input,
                         Output output )
        throws Exception {

        MapperTask task = new MapperTask();

        task.setInput( input );
        task.setOutput( output );

        task.init( config, work, delegate );

        daemon.getExecutorService( getClass() ).submit( task );

        trackTask( work, task );
        
    }
    
    protected Input readInput( Message message ) {
        return new Input( message.getList( "input" ) );

    }

    protected Output readOutput( Message message ) {
        return new Output( message.getList( "output" ) );
    }
    
    protected Work readWork( Input input, Message message ) {
        return new Work( input, message.getList( "work" ) );
    }
    
}
