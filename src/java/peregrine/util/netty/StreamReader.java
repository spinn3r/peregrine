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
package peregrine.util.netty;

import org.jboss.netty.buffer.*;

import peregrine.*;
import peregrine.os.*;
import java.io.*;
import java.util.*;

/**
 * <p>
 * Class which restricts the usage of a ChannelBuffer to JUST relative sequential
 * read operations.  This will enable us to support prefetch and mlock, fadvise
 * on a buffer as well as CRC32 after a page has been mlocked and easy to read.
 *
 * <p>
 * This is primarily used by a DefaultChunkReader so that all operations that it
 * needs to function can be easily provided (AKA CRC32 and prefetch/mlock).
 */
public class StreamReader {

    private ChannelBuffer buff = null;

    private StreamReaderListener listener = null;

    public StreamReader( ChannelBuffer buff ) {
        this.buff = buff;
    }

    /**
     * Read length `length' bytes from the reader.
     */
    public StructReader read( int length ) {
        fireOnRead( length );
        return new StructReader( buff.readSlice( length ) );
    }

    /**
     * Read a single byte from the stream.
     */
    public byte read() {
        fireOnRead(1);
        return buff.readByte();
    }

    public int readInt() {
        fireOnRead(4);
        return buff.readInt();
    }

    /**
     * Read a slice from this stream reader, as a COPY, note this copies data
     * into the heap.
     */
    private ChannelBuffer readSlice( int length ) {

        fireOnRead( length );

        byte[] result = new byte[ length ];
        buff.readBytes( result);

        return ChannelBuffers.wrappedBuffer( result );

    }
    
    /**
     * Return the current position in this stream.
     */
    public int index() {
    	return buff.readerIndex();
    }
    
    public StreamReaderListener getListener() { 
        return this.listener;
    }

    public void setListener( StreamReaderListener listener ) { 
        this.listener = listener;
    }

    /** 
     * Event signifying that we are about to read `length' additional bytes from
     * the stream.
     */
    private void fireOnRead( int length ) {

        if ( listener == null )
            return;

        listener.onRead( length );
        
    }

}
