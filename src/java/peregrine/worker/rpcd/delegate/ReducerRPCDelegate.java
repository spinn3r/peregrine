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

import java.util.concurrent.*;

import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.io.*;
import peregrine.io.driver.shuffle.*;
import peregrine.util.*;
import peregrine.worker.*;
import peregrine.task.*;

import com.spinn3r.log5j.*;

/**
 */
public class ReducerRPCDelegate extends MapperRPCDelegate {

    private static final Logger log = Logger.getLogger();

    @Override
    protected void exec( FSDaemon daemon,
                         Class delegate,
                         Config config,
                         Work work,
                         Input input,
                         Output output )
        throws Exception {

        ShuffleInputReference shuffleInput = (ShuffleInputReference)input.getReferences().get( 0 );
        log.info( "Using shuffle input : %s ", shuffleInput.getName() );

        ReducerTask task = new ReducerTask( config, work, delegate, shuffleInput );
        task.setInput( input );
        task.setOutput( output );

        daemon.getExecutorService( getClass() ).submit( task );

        trackTask( work, task );

    }

}
