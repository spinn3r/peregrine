
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

	/**
	 * The buffer where the given entry is stored.  A byte is fine as 255 100MB 
	 * files is a LOT of data. 
	 */
    private byte[] buffer;

    /**
     * The lookup for index to offset within the buffer to read this.  For a 
     * given entry, we can lookup the buffer and offset where it is stored.
     * 
     */
    private int[] lookup;

    private int idx = -1;

    private int start = 0;

    private int end = 0;

    private int size = 0;

    protected ChannelBuffer[] buffers;
 
    private KeyLookup( byte[] buffer,
    			       int[] lookup,
                       ChannelBuffer[] buffers ) {

        if ( buffer == null )
            throw new RuntimeException();
        
        this.lookup = new int[size];
        this.buffer = new byte[size];
        this.end = lookup.length - 1;
        this.size = lookup.length;
        this.buffers = buffers;
        
    }
    	
    public KeyLookup( int size,
                       ChannelBuffer[] buffers ) {

    	this( new byte[size], new int[size], buffers );
    	
    }

    public KeyLookup( ShuffleInputChunkReader reader, 
                      ChannelBuffer[] buffers )
        throws IOException {

        this( reader.size(), buffers );

        while ( reader.hasNext() ) {

            // advance the reader
            reader.next();

            // advance the lookup
            next();

            //FIXME this is incorrect but will work until I implement a MultiShuffleInputChunkReader
            KeyEntry entry = new KeyEntry( (byte)0, reader.getShufflePacket().getOffset() + reader.keyOffset() );
            entry.backing = buffers[0];
            
            set( entry );

        }

        reset();
        
    }

    public boolean hasNext() {
        return idx < end;
    }

    public void next() {
        ++idx;
    }

    /**
     *
     * @return The offset for the current item.
     */
    public int offset() {
        return lookup[idx];
    }

    /** 
     * Set the buffer and offset for the given entry.
     */
    public void set( KeyEntry entry ) {
    	buffer[idx] = entry.buffer;
        lookup[idx] = entry.offset;
    }

    public KeyEntry get() {
    	
    	KeyEntry result = new KeyEntry( buffer[idx], lookup[idx] );
    	result.backing = buffers[(int)result.buffer];

    	return result;
        
    }

    public int size() {
        return size;
    }
    
    public void reset() {
        this.idx = start - 1;
    }

    public byte[] key() {
        return get().read();
    }
    
    // zero copy slice implementation.
    public KeyLookup slice( int slice_start, int slice_end ) {

        KeyLookup slice = new KeyLookup( buffer, lookup, buffers );

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
