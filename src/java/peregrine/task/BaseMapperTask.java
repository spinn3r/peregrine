/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

import com.spinn3r.log5j.*;

/**
 * Base task for all task that read input from external systems or the 
 * filesystem (or pipes).  In practice this boils down to map and merge tasks.
 */
public abstract class BaseMapperTask extends BaseTask implements Callable {

    private static final Logger log = Logger.getLogger();

    /**
     * This tasks partition listeners.
     */
    protected List<ChunkStreamListener> listeners = new ArrayList();

    protected List<SequenceReader> jobInput = null;

    protected MapperChunkStreamListener mapperChunkStreamListener = null;
    
    /**
     * Run init just on Mapper and Merger tasks.
     */
    public void init( Config config, Work work, Class delegate ) throws IOException {

        mapperChunkStreamListener = new MapperChunkStreamListener();
        
        super.init( config, work, delegate );

        // make all shuffle dirs for the shuffle output paths to make sure we
        // have an empty set input for the shuffle data as opposed to missing
        // data which means we tried to read from something that didn't exist
        for( OutputReference ref : output.getReferences() ) {

            if ( ref instanceof ShuffleOutputReference ) {

                ShuffleOutputReference shuffleOutput = (ShuffleOutputReference)ref;

                String shuffle_dir = config.getShuffleDir( shuffleOutput.getName() );

                File dir = new File( shuffle_dir );

                if ( ! dir.exists() ) {
                    Files.mkdirs( dir );
                }
                
            }

        }

    }

    /**
     * Construct a set of ChunkReaders (one per input source) for the given
     * input.
     */
    protected List<SequenceReader> getJobInput() throws IOException {

        listeners.add( mapperChunkStreamListener );

        for( ShuffleJobOutput current : shuffleJobOutput ) {
            listeners.add( current );
        }

        List<SequenceReader> readers = new ArrayList();

        for( int i = 0; i < getInput().getReferences().size(); ++i ) {

        	InputReference inputReference  = getInput().getReferences().get( i );
        	WorkReference  workReference   = getWork().getReferences().get( i );
        	
            if ( inputReference instanceof BroadcastInputReference ) {
            	// right now we handle broadcast input differently.
                continue;
            }

            IODriver driver = IODriverRegistry.getInstance( inputReference.getScheme() );
            
            // see if it is registered as a driver.
            if ( driver != null ) {

                JobInput ji = driver.getJobInput( config, job, inputReference, workReference );
                ji.addListeners( listeners );
                
                readers.add( ji );
                continue;
            }

            throw new IOException( "Reference not supported: " + inputReference );
            
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
        return Base64.encode( SHA1.encode( String.format( "%s:%s:%s", config.getHost(), started, getPointer() ) ) );
        
    }

    /**
     * 
     * 
     * Make sure to always flush the output between chunks. This is only 1 flush
     * per every 100MB or so and isn't the end of the world.
     * 
     */
    class MapperChunkStreamListener implements ChunkStreamListener {

        public int lastChunk = -1;

        //private BaseMapper baseMapper = (BaseMapper)jobDelegate;
        
        public void onChunk( ChunkReference ref ) {

    	    log.info( "Handling chunk: %s for %s" , ref, getClass().getName() );

            ++lastChunk;
            
            // get the first nonce... 
            if ( nonce == null ) {
                nonce = getNonce();
            }

        }
        
        public void onChunkEnd( ChunkReference ref ) {

            try {

                // fire onChunkEnd on the mapper so that intermediate chunk data
                // can be sent to broadcast shuffles.
                // baseMapper.onChunkEnd();
                
                // first try to flush job output.
                new Flusher( getJobOutput() ).flush();

                pointer = getPointer();
                
                // update the nonce now ... 
                nonce = getNonce();
                
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }

        }

    }
    
}
