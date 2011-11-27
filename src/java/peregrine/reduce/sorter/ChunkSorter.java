
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

        FileInputStream inputStream   = new FileInputStream( input );
        FileOutputStream outputStream = new FileOutputStream( output );
        
        FileChannel inputChannel  = inputStream.getChannel();
        FileChannel outputChannel = outputStream.getChannel();

        try {
        
            log.info( "Going to sort: %s which is %,d bytes", input, input.length() );

            // TODO: do this async so that we can read from disk and compute at
            // the same time... we need a background thread to trigger the
            // pre-read.

            ShuffleInputChunkReader reader = new ShuffleInputChunkReader( config, partition, input.getPath() );
            
            ChannelBuffer buffer = reader.getBuffer();

            // we need our OWN copy of this buffer so that other thread don't
            // update the readerIndex and writerIndex
            buffer = buffer.slice( 0, buffer.writerIndex() );
            
            lookup = new KeyLookup( reader, buffer );

            log.info( "Key lookup for %s has %,d entries." , partition, lookup.size() );
            
            int depth = 0;

            ChunkReader result = null;

            lookup = sort( lookup, depth );
            
            //write this into the final ChunkWriter now.

            VarintReader varintReader = new VarintReader( buffer );

            ChunkWriter writer = new DefaultChunkWriter( output );

            while( lookup.hasNext() ) {

                // TODO: move this to use transferTo ... 

                lookup.next();

                int start = lookup.get() - 1;
                buffer.readerIndex( start );

                int key_length = varintReader.read();

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

            String error = String.format( "Unable to sort %s for %s" , input, partition );

            log.error( "%s", error, t );
            
            throw new IOException( error , t );
            
        } finally {
            
            inputChannel.close();
            outputChannel.close();

            inputStream.close();
            outputStream.close();
            
        }

    }

    private void transferTo( FileChannel inputChannel, FileChannel outputChannel, long position, long count )
        throws IOException {
        
        if ( inputChannel.transferTo( position, count, outputChannel ) != count )
            throw new IOException( "Incomplete write to: " + outputChannel );
    }

}
