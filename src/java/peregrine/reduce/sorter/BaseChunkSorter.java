
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
public class BaseChunkSorter {

    private static final Logger log = Logger.getLogger();

    //keeps track of the current input we're sorting.
    protected int id = 0;

    protected Config config;
    protected Partition partition;
    protected ShuffleInputReference shuffleInput;

    protected ChannelBuffer buffer = null;

    protected KeyLookup lookup = null;
    
    public BaseChunkSorter( Config config,
                            Partition partition,
                            ShuffleInputReference shuffleInput ) {

        this.config = config;
        this.partition = partition;
        this.shuffleInput = shuffleInput;

    }

    public ChunkReader sort( File input )
        throws IOException {

        try {
        
            log.info( "Going to sort: %s", input );

            FileInputStream fis = new FileInputStream( input );
            
            MappedByteBuffer map = fis.getChannel().map( FileChannel.MapMode.READ_ONLY, 0, input.length() );

            // FIXME: do this async so that we can read from disk and compute at
            // the same time.
            
            // force it into memory...
            map.load();

            // we prefer the channel buffer interface.
            buffer = ChannelBuffers.wrappedBuffer( map );

            DefaultChunkReader reader = new DefaultChunkReader( input, buffer );
            
            lookup = new KeyLookup( reader, buffer );

            int key_start = 0;
            int key_end   = reader.size() - 1;

            int depth = 0;

            ChunkReader result = null;

            //FIXME parse this into the final ChunkWriter now.
            lookup = sort( lookup, depth );

            while( lookup.hasNext() ) {

            }

            log.info( "Sort output file %s has %,d entries.", result, result.size() );

            return result;

        } catch ( Throwable t ) {
            t.printStackTrace();
            throw new IOException( t );
                
        }

    }

    public KeyLookup sort( KeyLookup input ) 
        throws IOException {

        int depth = 0;
        
        return sort( input, depth );

    }

    protected KeyLookup sort( KeyLookup input, int depth )
        throws IOException {

        if ( input.size <= 1 ) {
            return input;
        }

        int middle = input.size / 2; 

        KeyLookup left  = input.slice( 0, middle - 1 );
        KeyLookup right = input.slice( middle, input.size - 1 );

        left  = sort( left  , depth + 1 );
        right = sort( right , depth + 1 );

        // determine which merge structure to use... if this is the LAST merger
        // just write the results to disk.  Writing to memory and THEN writing
        // to disk would just waste CPU time.

        KeyLookup merged = merge( left, right, depth );

        return merged;
        
    }

    protected KeyLookup merge( KeyLookup left,
                               KeyLookup right,
                               int depth )
        throws IOException {
        
        KeyLookup result = new KeyLookup( left.size + right.size, left.buffer );
        
        // now merge both left and right and we're done.
        List<KeyLookup> list = new ArrayList( 2 );
        list.add( left );
        list.add( right );

        SorterPriorityQueue queue = new SorterPriorityQueue( list );

        while( true ) {
            
            SortQueueEntry entry = queue.poll();

            if ( entry == null )
                break;
            
            result.next();
            result.set( entry.ptr );
            
        }

        result.reset();
        return result;
        
    }

}
