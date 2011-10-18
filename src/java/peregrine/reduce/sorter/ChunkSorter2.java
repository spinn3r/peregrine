
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
import peregrine.reduce.merger.*;

import org.jboss.netty.buffer.*;

/**
 * 
 */
public class ChunkSorter2 {

    public ChunkReader sort( ChunkReader input )
        throws IOException {
        
        return sort( input, 0 );
        
    }

    public ChunkReader sort( ChunkReader input, int depth )
        throws IOException {

        if ( input.size() <= 1 ) {

            ChunkWriterIntermediate inter = new ChunkWriterIntermediate();

            if ( input.size() == 1 ) {
                inter.write( input.key(), input.value () );
            }
                
            // away with you scoundrel! 
            return inter.getChunkReader();
            
        }
        
        int middle = input.size() / 2; 

        ChunkReader left  = new ChunkReaderSlice( input, middle );
        ChunkReader right = new ChunkReaderSlice( input, input.size() - middle );

        left  = sort( left , depth + 1);
        right = sort( right, depth + 1 );

        // Determine which merge structure to use... if this is the LAST merger
        // just write the results to disk.  Writing to memory and THEN writing
        // to disk would just waste CPU time.

        ChunkWriterIntermediate merge = getChunkWriterIntermediate( depth, left, right );

        return merge( left, right, merge );
        
    }

    protected ChunkReader merge( ChunkReader left,
                                 ChunkReader right,
                                 ChunkWriterIntermediate merge )
        throws IOException {

        // now merge both left and right and we're done.

        List<ChunkReader> list = new ArrayList( 2 );
        list.add( left );
        list.add( right );

        MergerPriorityQueue queue = new MergerPriorityQueue( list );

        ChunkWriterIntermediate merged = new ChunkWriterIntermediate();

        while( true ) {
            
            MergeQueueEntry entry = queue.poll();

            if ( entry == null )
                break;

            merged.write( entry.key, entry.value );

        }

        return merged.getChunkReader();
        
    }

    protected ChunkWriterIntermediate getChunkWriterIntermediate( int depth,
                                                                  ChunkReader left,
                                                                  ChunkReader right )
        throws IOException {

        ChunkWriterIntermediate result = null;
        
        if ( depth == 0 ) {
            result = new ChunkWriterIntermediate();
        } else {

            // size the intermediate by looking at the size of the left and
            // right values which we can easily measure.

            /*
            int length = ((ChunkReaderSlice)left).length +
                         ((ChunkReaderSlice)right).length
                ; 
            
            result = new ChunkWriterIntermediate( length );
            */
            result = new ChunkWriterIntermediate();

        }

        return result;
        
    }
    
}
