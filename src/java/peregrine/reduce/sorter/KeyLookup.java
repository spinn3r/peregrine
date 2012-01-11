/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.reduce.sorter;

import java.io.*;
import peregrine.util.*;
import peregrine.util.primitive.LongBytes;
import peregrine.io.chunk.*;
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

    /**
     * The lookup for index to offset within the buffer to read this.  For a
     * given entry, we can lookup the buffer and offset where it is stored.  The
     * buffer and lookup data MUST be kept in a direct buffer NOT inside the JVM
     * and this memory MUST count against sortBufferSize.  For a 128MB buffer
     * using 8 byte keys and 8 byte values this would take 40MB of memory to
     * store the lookup data.
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
    
    public KeyLookup( CompositeChunkReader reader )
        throws IOException {

        this( reader.size(), reader.getBuffers().toArray( new ChannelBuffer[0] ) );

        while ( reader.hasNext() ) {

            // advance the reader
            reader.next();

            // advance the lookup
            next();

            ChunkReader delegate = reader.getChunkReader();
           
            KeyEntry entry = new KeyEntry( (byte)reader.index(), delegate.keyOffset() );
            
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

    public KeyLookup copy() {
        return slice( 0, size - 1 );
    }

    public void dump( String name ) {

        System.out.printf( "%s:\n", name );

        //copy it so we don't change the index... this doesn't need to be fast.
        KeyLookup copy = copy();

        System.out.printf( "\tindex:   %,d\n", copy.index );
        System.out.printf( "\tstart:   %,d\n", copy.start );
        System.out.printf( "\tend:     %,d\n", copy.end );
        System.out.printf( "\tsize:    %,d\n", copy.size );

        while( copy.hasNext() ) {
            copy.next();
            System.out.printf( "\t\t%s\n", Hex.encode( copy.key() ) );
        }

    }

}
