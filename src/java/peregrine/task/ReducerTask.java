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
package peregrine.task;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.map.*;
import peregrine.reduce.*;
import peregrine.sysstat.*;
import com.spinn3r.log5j.Logger;

public class ReducerTask extends BaseTask implements Callable {

    private static final Logger log = Logger.getLogger();
    
    private Reducer reducer;

    private ShuffleInputReference shuffleInput;

    private AtomicInteger nrTuples = new AtomicInteger();
    
    public ReducerTask( Config config,
                        Partition partition,
                        Class delegate,
                        ShuffleInputReference shuffleInput )
        throws Exception {

        super.init( partition );

        this.config = config;
        this.delegate = delegate;
        this.shuffleInput = shuffleInput;
        
    }

    public Object call() throws Exception {

        if ( output.getReferences().size() == 0 )
            throw new IOException( "Reducer tasks require output." );

        this.reducer = (Reducer)delegate.newInstance();

        SystemProfiler profiler = config.getSystemProfiler();

        try {

            log.info( "Running %s on %s", delegate, partition );
            
            setup();
            reducer.setBroadcastInput( BroadcastInputFactory.getBroadcastInput( config, getInput(), partition ) );
            reducer.init( getJobOutput() );

            try {
                doCall();
            } catch ( Throwable t ) {
                handleFailure( log, t );
            }

            try {
                reducer.cleanup();
            } catch ( Throwable t ) {
                handleFailure( log, t );
            }

            try {
                teardown();
            } catch ( Throwable t ) {
                handleFailure( log, t );
            }

        } catch ( Throwable t ) { 
            handleFailure( log, t );
        } finally {
            report();
            log.info( "Ran with profiler rate: \n%s", profiler.rate() );
        }

        return null;

    }

    private void doCall() throws Exception {
    	
    	SortListener listener = new ReducerTaskSortListener();
        
        LocalReducer reducer = new LocalReducer( config, partition, listener, shuffleInput, getJobOutput() );

        String shuffle_dir = config.getShuffleDir( shuffleInput.getName() );

        log.info( "Trying to find shuffle files in: %s", shuffle_dir );

        File shuffle_dir_file = new File( shuffle_dir );

        if ( ! shuffle_dir_file.exists() ) {
            throw new IOException( "Shuffle output does not exist: " + shuffleInput.getName() );
        }

        File[] shuffles = shuffle_dir_file.listFiles();

        //TODO: we should probably make sure these look like shuffle files.
        for( File shuffle : shuffles ) {
            reducer.add( shuffle );
        }
        
        int nr_readers = shuffles.length;

        reducer.sort();

        log.info( "Sorted %,d entries in %,d chunk readers for partition %s",
                  nrTuples , nr_readers, partition );

    }

    class ReducerTaskSortListener implements SortListener {
        
        public void onFinalValue( StructReader key, List<StructReader> values ) {

            try {

                reducer.reduce( key, values );
                nrTuples.getAndIncrement();

            } catch ( Exception e ) {
                throw new RuntimeException( "Reduce failed: " , e );
            }
                
        }

    }
    
}



