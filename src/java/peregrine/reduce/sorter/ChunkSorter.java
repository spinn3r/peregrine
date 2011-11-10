
package peregrine.reduce.sorter;

import java.io.*;
import java.nio.channels.*;

import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.shuffle.*;
import peregrine.util.*;
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

            ShuffleInputChunkReader reader = new ShuffleInputChunkReader( config, partition, input.getPath() );
            
            ChannelBuffer buffer = reader.getBuffer();

            lookup = new KeyLookup( reader, buffer );
            
            int depth = 0;

            ChunkReader result = null;

            lookup = sort( lookup, depth );
            
            //write this into the final ChunkWriter now.

            VarintReader varintReader = new VarintReader( buffer );

            ChunkWriter writer = new DefaultChunkWriter( output );

            System.out.printf( "FIXME: reading %s keys\n", lookup.size() );

            int idx = 0;
            while( lookup.hasNext() ) {

                // TODO: move this to use transferTo ... 

                lookup.next();

                int start = lookup.get() - 1;
                buffer.readerIndex( start );

                int key_length = varintReader.read();

                if ( key_length != 8 )
                    System.out.printf( "FIXME: %,d of %,d not 8: %s\n", idx, lookup.size(), key_length );

                byte[] key = new byte[ key_length ];
                buffer.readBytes( key );

                int value_length = varintReader.read();
                byte[] value = new byte[ value_length ];
                buffer.readBytes( value );

                writer.write( key, value );

                ++idx;
                
            }

            writer.close();
            
            log.info( "Sort output file %s has %,d entries.", output, reader.size() );

            result = new DefaultChunkReader( output );
            
            return result;

        } catch ( Throwable t ) {
            throw new IOException( String.format( "Unable to sort %s for %s" , input, partition ) , t );                
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
