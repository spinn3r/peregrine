
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
	 * Number of bytes needed to store a key in memory.  This is used so that 
	 * we can predict how much additional memory will be needed to store just 
	 * the pointer data.
	 */
	public static int KEY_SIZE = 5;
	
	// FIXME: the buffer and lookup data MUST be kept in a direct buffer NOT 
	// inside the JVM and this memory MUST count against sortBufferSize.  For a 
	// 128MB buffer using 8 byte keys and 8 byte values this would take 40MB of 
	// memory to store the lookup data.	

    /**
     * The lookup for index to offset within the buffer to read this.  For a 
     * given entry, we can lookup the buffer and offset where it is stored. 
     * 
     */
    private ChannelBuffer lookup;
    
    /**
     * The current index we're working on in the lookup and buffer structures.
     */
    private int index = -1;

    /**
     * For slices, this is the start of where we are working.
     */
    private int start = 0;

    /**
     * For slices, the end of where we are working.
     */
    private int end = 0;

    /**
     * The total number of items in this lookup.
     */
    private int size = 0;

    /**
     * The actual buffers storing the data.  Each entry has a buffer which we
     * resolve from `buffer` based on the index (index).
     */
    protected ChannelBuffer[] buffers;
 
    private KeyLookup( ChannelBuffer lookup, 
                       int size,
                       ChannelBuffer[] buffers ) {

        this.lookup = lookup;
        this.end = size - 1;
        this.size = size;
        this.buffers = buffers;
        
    }
    	
    public KeyLookup( int size,
                      ChannelBuffer[] buffers ) {

    	this( ChannelBuffers.directBuffer( size * KEY_SIZE ), size, buffers );
    	
    }

    public KeyLookup( CompositeShuffleInputChunkReader reader )
        throws IOException {

        this( reader.size(), reader.getBuffers().toArray( new ChannelBuffer[0] ) );

        while ( reader.hasNext() ) {

            // advance the reader
            reader.next();

            // advance the lookup
            next();

            ShuffleInputChunkReader delegate = reader.getShuffleInputChunkReader();
           
            KeyEntry entry = new KeyEntry( (byte)reader.index(), 
            		                       delegate.getShufflePacket().getOffset() + delegate.keyOffset() );
            
            entry.backing = reader.getBuffer();
            
            set( entry );

        }

        reset();
        
    }

    public boolean hasNext() {
        return index < end;
    }

    public void next() {
        ++index;
    }

    /** 
     * Set the buffer and offset for the given entry.
     */
    public void set( KeyEntry entry ) {
        
        lookup.writerIndex( index * KEY_SIZE );
        lookup.writeByte( entry.buffer );
        lookup.writeInt( entry.offset );
        
    }

    public KeyEntry get() {
    	
    	KeyEntry result = new KeyEntry();
    	
        lookup.readerIndex( index * KEY_SIZE );
        result.buffer  = lookup.readByte();
        result.offset  = lookup.readInt();
    	result.backing = buffers[(int)result.buffer];

    	return result;
        
    }

    public int size() {
        return size;
    }
    
    public void reset() {
        this.index = start - 1;
    }

    public byte[] key() {
        return get().read();
    }
    
    // zero copy slice implementation.
    public KeyLookup slice( int slice_start, int slice_end ) {

        KeyLookup slice = new KeyLookup( lookup, size, buffers );

        slice.size    = (slice_end - slice_start) + 1;
        
        slice.start   = this.start + slice_start;
        slice.index   = slice.start - 1;
        slice.end     = slice.start + slice.size - 1;
        
        return slice;
        
    }

    public KeyLookup clone() {
        return slice( 0, size - 1 );
    }

    public void dump( String name ) {

        System.out.printf( "%s:\n", name );

        //clone it so we don't change the index... this doesn't need to be fast.
        KeyLookup clone = clone();

        System.out.printf( "\tindex:   %,d\n", clone.index );
        System.out.printf( "\tstart:   %,d\n", clone.start );
        System.out.printf( "\tend:     %,d\n", clone.end );
        System.out.printf( "\tsize:    %,d\n", clone.size );

        while( clone.hasNext() ) {
            clone.next();
            System.out.printf( "\t\t%s\n", Hex.encode( clone.key() ) );
        }

    }

}
