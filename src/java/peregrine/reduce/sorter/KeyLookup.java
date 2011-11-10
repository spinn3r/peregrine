
package peregrine.reduce.sorter;

import java.io.*;
import peregrine.util.*;
import peregrine.util.primitive.LongBytes;
import peregrine.shuffle.*;

import org.jboss.netty.buffer.*;

/**
 * Maintains a key lookup system from the ChunkReader and backing ChannelBuffer
 * to the offset of the key in the channel buffer.  
 */
public class KeyLookup {

    int[] lookup;

    int idx = -1;

    int start = 0;

    int end = 0;

    int size = 0;

    ChannelBuffer buffer;
    
    public KeyLookup( int[] lookup,
                      ChannelBuffer buffer ) {

        if ( buffer == null )
            throw new RuntimeException();
        
        this.lookup = lookup;
        this.end = lookup.length - 1;
        this.size = lookup.length;
        this.buffer = buffer;
    }

    public KeyLookup( int size,
                      ChannelBuffer buffer ) {
        this( new int[size], buffer );
    }

    public KeyLookup( ShuffleInputChunkReader reader, 
                      ChannelBuffer buffer )
        throws IOException {

        this( reader.size(), buffer );

        while ( reader.hasNext() ) {

            // advance the reader
            reader.next();

            // advance the lookup
            next();

            set( reader.getShufflePacket().getOffset() + reader.keyOffset() );

        }

        reset();
        
    }

    public boolean hasNext() {
        return idx < end;
    }

    public void next() {
        ++idx;
    }

    public int offset() {
        return lookup[idx];
    }

    public void set( int value ) {
        lookup[idx] = value;
    }

    public int get() {
        return lookup[idx];
    }

    public int size() {
        return size;
    }
    
    public void reset() {
        this.idx = start - 1;
    }

    public byte[] key() {
        return key( get() );
    }

    public byte[] key( int ptr ) {
        byte[] data = new byte[LongBytes.LENGTH];
        buffer.getBytes( ptr, data );
        return data;
    }
    
    // zero copy slice implementation.
    public KeyLookup slice( int slice_start, int slice_end ) {

        KeyLookup slice = new KeyLookup( lookup, buffer );

        slice.size    = (slice_end - slice_start) + 1;
        
        slice.start   = this.start + slice_start;
        slice.idx     = slice.start - 1;
        slice.end     = slice.start + slice.size - 1;
        
        return slice;
        
    }

    public KeyLookup clone() {
        return slice( 0, size - 1 );
    }

    public void dump( String name ) {

        System.out.printf( "%s:\n", name );

        //clone it so we don't change the idx... this doesn't need to be fast.
        KeyLookup clone = clone();

        System.out.printf( "\tidx:   %,d\n", clone.idx );
        System.out.printf( "\tstart: %,d\n", clone.start );
        System.out.printf( "\tend:   %,d\n", clone.end );
        System.out.printf( "\tsize:  %,d\n", clone.size );

        while( clone.hasNext() ) {
            clone.next();
            System.out.printf( "\t\t%s\n", Hex.encode( clone.key() ) );
        }

    }

}
