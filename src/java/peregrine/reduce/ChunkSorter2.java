
package peregrine.reduce;

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

        ChunkWriterIntermediate merge = null;
        
        if ( depth == 0 ) {
            merge = new ChunkWriterIntermediate();
        } else {

            // size the intermediate by looking at the size of the left and
            // right values which we can easily measure.

            int length = ((ChunkReaderSlice)left).length +
                         ((ChunkReaderSlice)right).length
                ; 
            
            merge = new ChunkWriterIntermediate( length );

        }
        
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

}

class ChunkReaderSlice implements ChunkReader {

    private int size = 0;

    private int idx = 0;

    // approx length in bytes of this slice.
    protected int length = 0;
    
    private ChunkReader delegate;
    
    public ChunkReaderSlice( ChunkReader delegate , int size ) {
        this.delegate = delegate;
        this.size = size;
    }

    @Override
    public boolean hasNext() throws IOException {
        return idx < size;
    }

    @Override
    public byte[] key() throws IOException {
        byte[] result = delegate.key();
        length += result.length + IntBytes.LENGTH;
        return result;
    }

    @Override
    public byte[] value() throws IOException {

        // bump up pointer.
        ++idx;

        byte[] result = delegate.value();
        length += result.length + IntBytes.LENGTH;
        return result;

    }

    @Override
    public int size() throws IOException {
        return size;
    }

    @Override
    public void close() throws IOException {
        // noop
    }

}

class ChunkWriterIntermediate extends DefaultChunkWriter {

    public ChunkWriterIntermediate( int length ) throws IOException {
        super( new ByteArrayOutputStream( length ) );
    }

    public ChunkWriterIntermediate() throws IOException {
        this( 512 ) ;
    }

    public ChunkReader getChunkReader() throws IOException {

        close();
        
        ByteArrayOutputStream bos = (ByteArrayOutputStream)out;

        // FIXME ChannelBuffers would be WAY better because I don't have to call
        // toByteArray each time.
        return new DefaultChunkReader( bos.toByteArray() );

    }
    
}