
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

    public ChunkSorter2( Config config,
                         Partition partition,
                         ShuffleInputReference shuffleInput ) {
        this.config = config;
        this.partition = partition;
        this.shuffleInput = shuffleInput;
    }

    public ChunkReader sort( ChunkReader input )
        throws IOException {

        log.info( "Going to sort: %s", input );
        
        ChannelBuffer buff  = threadLocal.get();
        
        return sort( input, buff, 0 );
        
    }

    public ChunkReader sort( ChunkReader input,
                             ChannelBuffer buff,
                             int depth )
        throws IOException {

        if ( input.size() <= 1 ) {
            
            SorterIntermediate inter = new ChannelBufferSorterIntermediate( buff );

            ChunkWriter writer = inter.getChunkWriter();
            
            if ( input.size() == 1 && input.hasNext() ) {
                writer.write( input.key(), input.value () );
            }

            writer.close();
                
            // away with you scoundrel! 
            return inter.getChunkReader();
            
        }

        int middle = input.size() / 2; 

        ChunkReader left  = new ChunkReaderSlice( input, middle );
        ChunkReader right = new ChunkReaderSlice( input, input.size() - middle );

        left  = sort( left  , buff , depth + 1 );
        right = sort( right , buff , depth + 1 );

        // Determine which merge structure to use... if this is the LAST merger
        // just write the results to disk.  Writing to memory and THEN writing
        // to disk would just waste CPU time.

        SorterIntermediate merge = getSorterIntermediate( buff, depth );

        return merge( left, right, merge );
        
    }

    protected ChunkReader merge( ChunkReader left,
                                 ChunkReader right,
                                 SorterIntermediate merge )
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
    
}
