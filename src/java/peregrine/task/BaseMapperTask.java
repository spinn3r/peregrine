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

import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.driver.*;
import peregrine.io.driver.broadcast.*;
import peregrine.io.driver.file.*;
import peregrine.io.driver.shuffle.*;
import peregrine.io.partition.*;
import peregrine.io.util.*;
import peregrine.map.*;
import peregrine.shuffle.sender.*;
import peregrine.util.*;

/**
 * Base task for all task that read input from external systems or the 
 * filesystem (or pipes).  In practice this boils down to map and merge tasks.
 */
public abstract class BaseMapperTask extends BaseTask implements Callable {

    /**
     * This tasks partition listeners.
     */
    protected List<ChunkStreamListener> listeners = new ArrayList();

    protected List<SequenceReader> jobInput = null;

    /**
     * The current nonce computed from the current input position.
     */
    protected String nonce = null;
    
    /**
     * Run init just on Mapper and Merger tasks.
     */
    public void init( Config config, Work work, Partition partition, Class delegate ) throws IOException {

        super.init( config, work, partition, delegate );

        // make all shuffle dirs for the shuffle output paths to make sure we
        // have an empty set input for the shuffle data as opposed to missing
        // data which means we tried to read from something that didn't exist
        for( OutputReference ref : output.getReferences() ) {

            if ( ref instanceof ShuffleOutputReference ) {

                ShuffleOutputReference shuffleOutput = (ShuffleOutputReference)ref;

                String shuffle_dir = config.getShuffleDir( shuffleOutput.getName() );

                File dir = new File( shuffle_dir );

                if ( ! dir.exists() ) {
                    dir.mkdirs();
                }
                
            }

        }

    }

    /**
     * Construct a set of ChunkReaders (one per input source) for the given
     * input.
     */
    protected List<SequenceReader> getJobInput() throws IOException {

        for( ShuffleJobOutput current : shuffleJobOutput ) {
            listeners.add( current );
        }

        listeners.add( new MapperChunkStreamListener() );
        
        List<SequenceReader> readers = new ArrayList();
        
        for( InputReference ref : getInput().getReferences() ) {

            if ( ref instanceof BroadcastInputReference ) {
            	// right now we handle broadcast input differently.
                continue;
            }

            IODriver driver = IODriverRegistry.getInstance( ref.getScheme() );
            
            // see if it is registered as a driver.
            if ( driver != null ) {

                JobInput ji = driver.getJobInput( config, ref, new PartitionWorkReference( partition ) );
                ji.addListeners( listeners );
                
                readers.add( ji );
                continue;
            }

            throw new IOException( "Reference not supported: " + ref );
            
        }

        return readers;
        
    }

    protected String getPointer() {

        List<String> pointers = new ArrayList();

        for( SequenceReader reader : jobInput ) {

            if ( reader instanceof LocalPartitionReader )
                pointers.add( reader.toString() );
            
        }

        return Strings.join( pointers, "," );
        
    }

    protected String getNonce() {

        // host, job start time, and the pointer we are on
        return Base16.encode( SHA1.encode( String.format( "%s:%s:%s", config.getHost(), started, getPointer() ) ) );
        
    }
    
    /**
     * 
     * 
     * Make sure to always flush the output between chunks. This is only 1 flush
     * per every 100MB or so and isn't the end of the world.
     * 
     */
    class MapperChunkStreamListener implements ChunkStreamListener{

        public void onChunk( ChunkReference ref ) {

            // get the first nonce... 
            if ( nonce == null ) {
                nonce = getNonce();
            }

        }
        
        public void onChunkEnd( ChunkReference ref ) {

            try {

                // first try to flush job output.
                new Flusher( getJobOutput() ).flush();

                // now send progress to the controller.
                sendProgressToController( nonce, getPointer() );

                // update the nonce now ... 
                nonce = getNonce();
                
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }

        }

    }
    
}
