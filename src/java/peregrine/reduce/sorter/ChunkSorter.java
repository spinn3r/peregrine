
package peregrine.reduce.sorter;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.partition.*;
import peregrine.reduce.merger.*;

import org.jboss.netty.buffer.*;

import com.spinn3r.log5j.Logger;

/**
 * 
 */
public class ChunkSorter extends BaseChunkSorter {

    private static final Logger log = Logger.getLogger();

    protected ChannelBuffer buffer = null;

    public ChunkSorter( Config config,
                        Partition partition,
                        ShuffleInputReference shuffleInput ) {

        super( config, partition, shuffleInput );
        
    }

    public ChunkReader sort( File input, File output )
        throws IOException {

        try {
        
            log.info( "Going to sort: %s", input );

            FileChannel inputChannel  = new FileInputStream( input  ).getChannel();
            FileChannel outputChannel = new FileOutputStream( output ).getChannel();
            
            MappedByteBuffer inputMap  = inputChannel.map(  FileChannel.MapMode.READ_ONLY,  0, input.length() );

            // we prefer the channel buffer interface.
            buffer = ChannelBuffers.wrappedBuffer( inputMap );

            // TODO: do this async so that we can read from disk and compute at
            // the same time... we need a background thread to trigger the
            // pre-read.

            DefaultChunkReader reader = new DefaultChunkReader( input, buffer );

            lookup = new KeyLookup( reader, buffer );

            int key_start = 0;
            int key_end   = reader.size() - 1;

            int depth = 0;

            ChunkReader result = null;

            lookup = sort( lookup, depth );

            //parse this into the final ChunkWriter now.

            VarintReader varintReader = new VarintReader( buffer );
            
            while( lookup.hasNext() ) {

                lookup.next();

                // TODO: this would be BAD if the key size was > 8 bytes. 
                int start = lookup.get() - 1;
                buffer.readerIndex( start );

                //jump past the key
                buffer.readerIndex( buffer.readerIndex() + varintReader.read() + 1 );
                //jump past the value
                buffer.readerIndex( buffer.readerIndex() + varintReader.read() );

                //now read the range inclusive
                int end = buffer.readerIndex();

                int count = (end - start) + 1;
                
                transferTo( inputChannel, outputChannel, start, count );
                
            }

            // now write out the length and we are done
            transferTo( inputChannel, outputChannel, input.length() - 4, IntBytes.LENGTH );

            log.info( "Sort output file %s has %,d entries.", output, reader.size() );

            return result;

        } catch ( Throwable t ) {
            t.printStackTrace();
            throw new IOException( t );
                
        }

    }

    private void transferTo( FileChannel inputChannel, FileChannel outputChannel, long position, long count )
        throws IOException {
        
        if ( inputChannel.transferTo( position, count, outputChannel ) != count )
            throw new IOException( "Incomplete write to: " + outputChannel );
    }

}
