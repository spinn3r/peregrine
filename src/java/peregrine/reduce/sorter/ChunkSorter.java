
package peregrine.reduce.sorter;

import java.io.*;

import java.nio.channels.*;
import java.util.*;

import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.util.*;
import peregrine.shuffle.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.values.*;

import org.jboss.netty.buffer.*;

import com.spinn3r.log5j.Logger;

/**
 * 
 */
public class ChunkSorter extends BaseChunkSorter {

    private static final Logger log = Logger.getLogger();

    public ChunkSorter( Config config,
                        Partition partition,
                        ShuffleInputReference shuffleInput ) {

        super( config, partition, shuffleInput );
        
    }

    public ChunkReader sort( List<ShuffleInputChunkReader> input, File output, List<JobOutput> jobOutput )
        throws IOException {

        CompositeShuffleInputChunkReader reader = null;

        ChunkWriter writer = null;

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

            while( lookup.hasNext() ) {

                // TODO: move this to use transferTo ... 

                lookup.next();

                KeyEntry current = lookup.get();
                
                VarintReader varintReader = new VarintReader( current.backing );
                
                int start = current.offset - 1;
                current.backing.readerIndex( start );

                int key_length = varintReader.read();
                StructReader key = new StructReader( current.backing.readSlice( key_length ) );

                int value_length = varintReader.read();
                StructReader value = new StructReader( current.backing.readSlice( value_length ) );

                writer.write( key, value );
                
            }

            log.info( "Sort output file %s has %,d entries.", output, lookup.size() );

        } catch ( Throwable t ) {

            String error = String.format( "Unable to sort %s for %s" , input, partition );

            log.error( "%s", error, t );
            
            throw new IOException( error , t );
            
        } finally {

            new Flusher( jobOutput )
                .flush();

            new Closer( reader, writer ).close();

        }

        // if we got to this part we're done... 
        return new DefaultChunkReader( config, output );

    }

    private void transferTo( FileChannel inputChannel, FileChannel outputChannel, long position, long count )
        throws IOException {
        
        if ( inputChannel.transferTo( position, count, outputChannel ) != count )
            throw new IOException( "Incomplete write to: " + outputChannel );
    }

}
