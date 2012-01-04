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
import peregrine.config.Config;
import peregrine.config.Host;
import peregrine.config.Membership;
import peregrine.config.Partition;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.partition.*;
import peregrine.io.util.*;
import peregrine.map.*;
import peregrine.shuffle.sender.*;

public abstract class BaseMapperTask extends BaseTask implements Callable {

    /**
     * This tasks partition listeners.
     */
    protected List<LocalPartitionReaderListener> listeners = new ArrayList();

    /**
     * Construct a set of ChunkReaders (one per input source) for the given
     * input.
     */
    protected List<ChunkReader> getJobInput() throws IOException {

        for( ShuffleJobOutput current : shuffleJobOutput ) {
            listeners.add( current );
        }

        listeners.add( new FlushLocalPartitionReaderListener() );
        
        List<ChunkReader> readers = new ArrayList();
        
        for( InputReference ref : getInput().getReferences() ) {

            if ( ref instanceof BroadcastInputReference ) {
            	// right now we handle broadcast input differently.
                continue;
            }
            
            if ( ref instanceof FileInputReference ) {
                FileInputReference file = (FileInputReference) ref;
                readers.add( new LocalPartitionReader( config, partition, file.getPath(), listeners ) );
            }
            
            throw new IOException( "Reference not supported: " + ref );
            
        }

        return readers;
        
    }

    /**
     * Make sure to always flush the output between chunks. This is only 1 
     * flush per every 100MB or so and isn't the end of the world.
     * 
     */
    class FlushLocalPartitionReaderListener implements LocalPartitionReaderListener{

        public void onChunk( ChunkReference ref ) { }
        
        public void onChunkEnd( ChunkReference ref ) {

            try {
                new Flusher( getJobOutput() ).flush();
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }

        }

    }
    
}
