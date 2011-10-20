
package peregrine.reduce.sorter;

import java.io.*;
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
public class ChunkSorter2 {

    private static final Logger log = Logger.getLogger();

    public static int EXTENT_LENGTH = 4194304;
    
    private static ThreadLocalChannelBuffer threadLocal =
        new ThreadLocalChannelBuffer( (int)DefaultPartitionWriter.CHUNK_SIZE, EXTENT_LENGTH );

    //keeps track of the current input we're sorting.
    private int id = 0;

    private Config config;
    private Partition partition;
    private ShuffleInputReference shuffleInput;

    private ChannelBuffer buffer;
    
    public ChunkSorter2( Config config,
                         Partition partition,
                         ShuffleInputReference shuffleInput ) {

        this.config = config;
        this.partition = partition;
        this.shuffleInput = shuffleInput;

        this.buffer   = threadLocal.initialValue();

    }

    public ChunkReader sort( ChunkReader input )
        throws IOException {

        try {
        
            log.info( "Going to sort: %s", input );
            
            ChunkReader result = sort( input, 0, 0 );
            
            log.info( "Sort output file %s has %,d entries.", result, result.size() );

            return result;

        } catch ( Throwable t ) {
            t.printStackTrace();
            throw new IOException( t );
                
        }

    }

    private ChunkReader sort( ChunkReader input, int depth, int index )
        throws IOException {

        // FIXME: what I should do is EACH STEP after this I should hex dump the
        // FULL array so I can understand what is being written into it... AND
        // the pointers of the chunk readers.... so that I can look at their raw
        // values in the given range.
        
        if ( input.size() <= 1 ) {
            
            SorterIntermediate inter = new ChannelBufferSorterIntermediate( buffer );

            ChunkWriter writer = inter.getChunkWriter();
            
            if ( input.size() == 1 && input.hasNext() ) {

                byte[] key   = input.key();
                byte[] value = input.value();

                writer.write( key, value );
            }

            writer.close();
                
            // away with you scoundrel! 
            return inter.getChunkReader();
            
        }

        int middle = input.size() / 2; 

        ChunkReader left  = new ChunkReaderSlice( input, middle );
        ChunkReader right = new ChunkReaderSlice( input, input.size() - middle );

        left  = sort( left  , depth + 1, buffer.writerIndex() );
        right = sort( right , depth + 1, buffer.writerIndex() );

        dump( "left",  left );
        dump( "right", right );
        
        // Determine which merge structure to use... if this is the LAST merger
        // just write the results to disk.  Writing to memory and THEN writing
        // to disk would just waste CPU time.

        SorterIntermediate merge = getSorterIntermediate( buffer, depth );

        ChunkReader merged = merge( left, right, merge, depth );

        dump( "merged", merged );

        buffer.readerIndex( index );
        buffer.writerIndex( index );
        
        return merged;
        
    }

    protected ChunkReader merge( ChunkReader left,
                                 ChunkReader right,
                                 SorterIntermediate merge,
                                 int depth )
        throws IOException {

        // now merge both left and right and we're done.

        List<ChunkReader> list = new ArrayList( 2 );
        list.add( left );
        list.add( right );

        MergerPriorityQueue queue = new MergerPriorityQueue( list );

        ChunkWriter writer = merge.getChunkWriter();
        
        while( true ) {
            
            MergeQueueEntry entry = queue.poll();

            if ( entry == null )
                break;
            
            writer.write( entry.key, entry.value );

        }

        writer.close();
        
        return merge.getChunkReader();
        
    }

    protected SorterIntermediate getSorterIntermediate( ChannelBuffer buff, int depth )
        throws IOException {

        SorterIntermediate result = null;
        
        if ( depth == 0 ) {

            String relative = String.format( "/tmp/%s/sort-%s.tmp" , shuffleInput.getName(), id++ );
            
            String path = config.getPath( partition, relative );

            result = new PersistentSorterIntermediate( path );
            
        } else {
            result = new ChannelBufferSorterIntermediate( buff );
        }

        return result;
        
    }

    private void dump( String name, ChunkReader reader ) throws IOException {

        if ( reader instanceof ChannelBufferChunkReader ) {

            reader = ((ChannelBufferChunkReader)reader).duplicate();
            
            System.out.printf( "%s:\n", name );
            
            while( reader.hasNext() ) {

                byte[] key = reader.key();
                byte[] value = reader.value();

                System.out.printf( "dump(): key: %s\n", Hex.encode( key ) );
                
            }
            
            // so something else can look at it.
            reader.close();

        }

    }

}
