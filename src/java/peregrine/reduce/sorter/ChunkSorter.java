
package peregrine.reduce.sorter;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import java.nio.*;
import java.nio.channels.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.partition.*;
import peregrine.keys.*;
import peregrine.map.*;
import peregrine.reduce.merger.*;
import peregrine.shuffle.*;
import peregrine.util.*;
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

    public ChunkReader sort( File input, File output )
        throws IOException {
        
        FileChannel inputChannel  = new FileInputStream( input  ).getChannel();
        FileChannel outputChannel = new FileOutputStream( output ).getChannel();

        try {
        
            log.info( "Going to sort: %s", input );

            // TODO: do this async so that we can read from disk and compute at
            // the same time... we need a background thread to trigger the
            // pre-read.

            ShuffleInputChunkReader reader = new ShuffleInputChunkReader( input.getPath(), partition.getId() );

            ChannelBuffer buffer = reader.getShuffleInputReader().getBuffer();

            lookup = new KeyLookup( reader, buffer );
            
            int key_start = 0;
            int key_end   = reader.size() - 1;

            int depth = 0;

            ChunkReader result = null;

            lookup = sort( lookup, depth );
            
            //parse this into the final ChunkWriter now.

            VarintReader varintReader = new VarintReader( buffer );

            ChunkWriter writer = new DefaultChunkWriter( output );
            
            while( lookup.hasNext() ) {

                lookup.next();

                // TODO: this would be BAD if the key size was > 8 bytes. 
                int start = lookup.get() - 1;
                buffer.readerIndex( start );

                int key_length = varintReader.read();

                if ( key_length != 8 )
                    throw new RuntimeException( String.format( "Key length is incorrect for %s on partition %s", input, partition ) );
                
                byte[] key = new byte[ key_length ];
                buffer.readBytes( key );

                int value_length = varintReader.read();
                byte[] value = new byte[ value_length ];
                buffer.readBytes( value );

                writer.write( key, value );

            }

            writer.close();
            
            log.info( "Sort output file %s has %,d entries.", output, reader.size() );

            result = new DefaultChunkReader( output );
            
            return result;

        } catch ( Throwable t ) {
            t.printStackTrace();
            throw new IOException( t );
                
        } finally {
            inputChannel.close();
            outputChannel.close();
        }

    }

    private void transferTo( FileChannel inputChannel, FileChannel outputChannel, long position, long count )
        throws IOException {
        
        if ( inputChannel.transferTo( position, count, outputChannel ) != count )
            throw new IOException( "Incomplete write to: " + outputChannel );
    }

}
