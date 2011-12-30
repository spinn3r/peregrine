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

import java.io.*;
import java.util.*;

import peregrine.util.*;
import peregrine.util.netty.*;

import org.jboss.netty.buffer.*;


/**
 * Buffer output until it is ready to be sent in one CHUNK.  Not N chunks.  This
 * uses ChannelBuffers which can be more efficient as they are all sliced and
 * then an aggregate is used.
 * 
 */
public class BufferedChannelBufferWritable implements ChannelBufferWritable {

    public List<ChannelBuffer> buffers = new ArrayList();

    /**
     * The length, in bytes, for the pending write.
     */
    private int writeLength = 0;

    private int capacity;

    private ChannelBufferWritable delegate;

    private boolean closed = false;
    
    public BufferedChannelBufferWritable( ChannelBufferWritable delegate,
                                          int capacity ) {
        this.delegate = delegate;
        this.capacity = capacity;
    }

    @Override
    public void write( ChannelBuffer buff ) throws IOException {

        if ( closed )
            throw new IOException( "closed" );
        
        if ( ! hasCapacity( buff ) ) { 
            flush();
        }

        buffers.add( buff );
        writeLength += buff.writerIndex();

    }

    /**
     * Perform an action preFlush.  This could be used for example to add
     * additional metadata to a write before we flush the data.
     */
    public void preFlush() throws IOException { }

    @Override
    public void flush() throws IOException {
        flush( true );
    }
    
    /**
     * Write out all buffers to the delegate.
     */
    public void flush( boolean flushDelegate ) throws IOException {

        if ( writeLength == 0 )
            return;

        if ( closed )
            return;

        if ( buffers.size() == 0 ) {
            return;
        }

        preFlush();

        ChannelBuffer writeBuffer = getChannelBuffer();

        delegate.write( writeBuffer );
        
        buffers = new ArrayList();
        writeLength = 0;

        // flush the underlying delegate too
        if ( flushDelegate ) 
            delegate.flush();
        
    }

    @Override
    public void shutdown() throws IOException {
        flush();
        delegate.shutdown();
    }

    @Override
    public void sync() throws IOException {
        flush();
    }

    @Override
    public void close() throws IOException {

        if ( closed )
            return;
        
        flush();

        delegate.close();

        closed = true;

    }

    @Override
    public String toString() {
        return delegate.toString();
    }
    
    protected ChannelBuffer getChannelBuffer() {

        ChannelBuffer[] buffs = new ChannelBuffer[ buffers.size() ];
        buffs = buffers.toArray( buffs );

        return ChannelBuffers.wrappedBuffer( buffs );
        
    }

    /**
     * Make sure we can accept this output buffer so that it's < 16384 but as
     * close as possible.
     */
    protected boolean hasCapacity( ChannelBuffer buff ) {

        int newCapacity = writeLength + buff.writerIndex();
        
        return newCapacity < capacity;

    }
    
}
