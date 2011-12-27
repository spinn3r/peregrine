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
 */
public class MapperRPCDelegate extends RPCDelegate<FSDaemon> {

    private static final Logger log = Logger.getLogger();

    public void handleMessage( FSDaemon daemon, Channel channel, Message message )
        throws Exception {
    	
        String action = message.get( "action" );

        if ( "exec".equals( action ) ) {

            log.info( "Going to map from action: %s", message );

            Input input            = readInput( message );
            Output output          = readOutput( message );
            Partition partition    = new Partition( message.getInt( "partition" ) );
            Class delegate         = Class.forName( message.get( "delegate" ) );
            Config config          = daemon.getConfig();

            exec( daemon, delegate, config, partition, input, output );
            
            return;

        }

        throw new Exception( String.format( "No handler for action %s with message %s", action, message ) );

    }

    protected void exec( FSDaemon daemon,
                         Class delegate,
                         Config config,
                         Partition partition,
                         Input input,
                         Output output )
        throws Exception {

        MapperTask task = new MapperTask();
        
        task.init( config, config.getMembership(), partition, config.getHost(), delegate );
        
        task.setInput( input );
        task.setOutput( output );

        log.info( "Running delegate %s with input %s and output %s", delegate.getName(), input, output );

        daemon.getExecutorService( getClass() ).submit( task );

    }
    
    protected Input readInput( Message message ) {

        return new Input( readList( message, "input." ) );
        
    }

    protected Output readOutput( Message message ) {

        return new Output( readList( message, "output." ) );

    }

    protected List<String> readList( Message message, String prefix ) {

        List<String> result = new ArrayList();
    
        for( int i = 0 ; i < Integer.MAX_VALUE; ++i ) {

            String val = message.get( prefix + i );

            if ( val == null )
                break;

            result.add( val );
            
        }

        return result;
        
    }
    
}
