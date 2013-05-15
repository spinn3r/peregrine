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
import peregrine.os.*;
import peregrine.sort.*;

import org.jboss.netty.buffer.*;

import com.spinn3r.log5j.Logger;

/**
 * Sort the given chunk readers based on the key.
 */
public class ChunkSorter extends BaseChunkSorter {

    private static final Logger log = Logger.getLogger();
    
    private Partition partition;
    
    //keeps track of the current input we're sorting.
    private int id = 0;

    private Config config;

    private Job job = null;
    
    public ChunkSorter( Config config,
                        Partition partition,
                        Job job,
                        SortComparator comparator ) {

        super( comparator );

    	this.config = config;
		this.partition = partition;
        this.job = job;

    }

    public SequenceReader sort( List<ChunkReader> input,
                                File output,
                                List<JobOutput> jobOutput )
        throws IOException {

        return sort( input, output, jobOutput, null );
        
    }
    
    public SequenceReader sort( List<ChunkReader> input,
                                File output,
                                List<JobOutput> jobOutput,
                                SortListener sortListener )
        throws IOException {

        CompositeChunkReader reader    = null;
        DefaultChunkWriter writer      = null;
        DefaultChunkWriter sortWriter  = null;
        SortResult sortResult          = null;
        KeyLookup lookup               = null;
        KeyLookup sorted               = null;
        
        try {

            log.info( "Going to sort: %s (memory %s)", input, DirectMemoryAllocationTracker.getInstance() );

            // TODO: do this async so that we can read from disk and compute at
            // the same time... we need a background thread to trigger the
            // pre-read.
            
            reader = new CompositeChunkReader( config, input );
            
            lookup = new KeyLookup( reader );

            log.debug( "Key lookup for %s has %,d entries." , partition, lookup.size() );

            try {
                sorted = sort( lookup );
            } finally {
                lookup.close();
            }
            
            //write this into the final ChunkWriter now.

            if ( output != null ) {
                writer = new DefaultChunkWriter( config, output );
                sortWriter = writer;
            }

            // setup a combiner here... instantiate the Combiner and call
            //
            // init( Job, List<JobOutput> )
            //
            // and we don't need to pass the writer as we can just emit() from
            // the combiner.

            if ( job.getCombiner() != null ) {

                final Reducer combiner = newCombiner( writer );

                sortListener = new SortListener() {

                        public void onFinalValue( StructReader key, List<StructReader> values ) {
                            combiner.reduce( key, values );
                        }

                    };

                // set the sort writer to null so that SortResult doesn't have a
                // writer which means that it functions just to call
                // onFinalValue.  TODO: in the future it might be nice to make
                // SortResult ONLY work via this method so that the code is
                // easier to maintain.
                
                sortWriter = null;
                
            }
            
            sortResult = new SortResult( sortWriter, sortListener );

            KeyLookupReader keyLookupReader = new KeyLookupReader( sorted );

            while( keyLookupReader.hasNext() ) {

            	keyLookupReader.next();

                StructReader key = keyLookupReader.key();

                StructReader value = keyLookupReader.value();

                sortResult.accept( new SortEntry( key, value ) );

            }

            log.debug( "Sort output file %s has %,d entries. (memory %s)", output, sorted.size(), DirectMemoryAllocationTracker.getInstance() );

        } catch ( Throwable t ) {

            String error = String.format( "Unable to sort %s for %s" , input, partition );

            log.error( "%s", error, t );
            
            throw new IOException( error , t );
            
        } finally {

            //TODO: these need to be the same flusher.
            new Flusher( jobOutput ).flush();

            new Flusher( writer ).flush();
            
            // NOTE: it is important that the writer be closed before the reader
            // because if not then the writer will attempt to read values from 
            // closed reader.
            new Closer( sortResult, writer, reader, sorted ).close();

        }

        // if we got to this part we're done... 

        DefaultChunkReader result = null;
        
        if ( output != null ) {
            result = new DefaultChunkReader( config, output );
        }

        return result;

    }

    /**
     * Create a combiner instance and return it as a Reducer (they use the same
     * interface).
     */
    public Reducer newCombiner( DefaultChunkWriter writer ) {

        try {
            List<JobOutput> jobOutput = new ArrayList();
            jobOutput.add( new CombinerJobOutput( writer ) );
            
            Reducer result = (Reducer)job.getCombiner().newInstance();
            result.init( job, jobOutput );

            return result;
            
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
        
    }

}

class CombinerJobOutput implements JobOutput {

    private DefaultChunkWriter writer;
    
    public CombinerJobOutput( DefaultChunkWriter writer ) {
        this.writer = writer;
    }

    @Override 
    public void emit( StructReader key, StructReader value ) {

        try {

            // NOTE: that the merger expects to read a varint on the number of
            // items stored here so we need to 'wrap' it even though it's a
            // single value.  This is a slight overhead but we should probably
            // ignore it.  Most combiner use will probably take N input items
            // and map to 1 output item.  In this case we don't care if there is
            // a 1 byte varint overhead.
            
            writer.write( key, StructReaders.wrap( value ) );

        } catch ( IOException e ) {
            throw new RuntimeException( e ) ;
        }

    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }

}