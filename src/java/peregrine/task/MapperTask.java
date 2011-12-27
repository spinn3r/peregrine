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

import peregrine.*;
import peregrine.io.chunk.*;
import peregrine.io.partition.*;
import peregrine.io.util.*;
import peregrine.map.*;
import peregrine.sysstat.*;

import com.spinn3r.log5j.*;

public class MapperTask extends BaseMapperTask {

    private static final Logger log = Logger.getLogger();

    private Mapper mapper;

    public Object call() throws Exception {

        mapper = (Mapper)super.newMapper();

        SystemProfiler profiler = config.getSystemProfiler();

        try {

            log.info( "Running %s on %s", delegate, partition );
            
            setup();
            mapper.setBroadcastInput( getBroadcastInput() );
            mapper.init( getJobOutput() );

            try {
                doCall();
            } catch ( Throwable t ) {
                handleFailure( log, t );
            }

            try {
                mapper.cleanup();
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

        // note a map job with ZERO input files is acceptable.  This would be
        // used for some generator that just emits values on init.
        
        if ( getInput().getReferences().size() > 1 ) {
            throw new Exception( "Map jobs must not have more than one input." );
        }

        List<LocalPartitionReader> readers = getLocalPartitionReaders();

        if ( readers.size() == 0 )
            return;
        
        LocalPartitionReader reader = readers.get( 0 );

        reader.addListener( new LocalPartitionReaderListener() {

                public void onChunk( ChunkReference ref ) { }
                
                public void onChunkEnd( ChunkReference ref ) {

                    try {
                        new Flusher( jobOutput ).flush();
                    } catch ( IOException e ) {
                        throw new RuntimeException( e );
                    }

                }

            } );
        
        int count = 0;
        
        while( reader.hasNext() ) {
            mapper.map( reader.key(), reader.value() );
            ++count;
        }

        log.info( "Mapped %,d entries on %s on host %s from %s", count, partition, config.getHost(), reader );

    }

}
