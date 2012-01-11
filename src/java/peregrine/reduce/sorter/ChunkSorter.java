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
package peregrine.reduce.sorter;

import java.io.*;

import java.nio.channels.*;
import java.util.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.driver.shuffle.*;
import peregrine.io.util.*;
import peregrine.reduce.*;
import peregrine.shuffle.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import org.jboss.netty.buffer.*;

import com.spinn3r.log5j.Logger;

/**
 * Sort the given chunk readers based on the key.
 * 
 */
public class ChunkSorter extends BaseChunkSorter {

    private static final Logger log = Logger.getLogger();

    public ChunkSorter( Config config,
                        Partition partition,
                        ShuffleInputReference shuffleInput ) {

        super( config, partition, shuffleInput );
        
    }

    public SequenceReader sort( List<ChunkReader> input, File output, List<JobOutput> jobOutput )
        throws IOException {

        CompositeShuffleInputChunkReader reader = null;

        ChunkWriter writer = null;

        SortResult sortResult = null;
        
        try {

            log.info( "Going to sort: %s", input );

            // TODO: do this async so that we can read from disk and compute at
            // the same time... we need a background thread to trigger the
            // pre-read.
            
            reader = new CompositeShuffleInputChunkReader( config, partition, input );
            
            lookup = new KeyLookup( reader );

            log.info( "Key lookup for %s has %,d entries." , partition, lookup.size() );
            
            int depth = 0;

            lookup = sort( lookup, depth );
            
            //write this into the final ChunkWriter now.

            writer = new DefaultChunkWriter( config, output );

            sortResult = new SortResult( writer );
            
            while( lookup.hasNext() ) {

                lookup.next();

                KeyEntry current = lookup.get();

                ChannelBuffer backing = current.backing;
                
                VarintReader varintReader = new VarintReader( backing );
                
                int start = current.offset - 1;
                backing.readerIndex( start );

                StructReader key =
                    new StructReader( backing.readSlice( varintReader.read() ) );

                StructReader value =
                    new StructReader( backing.readSlice( varintReader.read() ) );

                sortResult.accept( new SortEntry( key, value ) );
                
            }

            log.info( "Sort output file %s has %,d entries.", output, lookup.size() );

        } catch ( Throwable t ) {

            String error = String.format( "Unable to sort %s for %s" , input, partition );

            log.error( "%s", error, t );
            
            throw new IOException( error , t );
            
        } finally {

            new Flusher( jobOutput ).flush();

            // NOTE: it is important that the writer be closed before the reader
            // because if not then the writer will attempt to read values from 
            // closed reader.
            new Closer( sortResult, writer, reader ).close();

        }

        // if we got to this part we're done... 
        return new DefaultChunkReader( config, output );

    }

}
