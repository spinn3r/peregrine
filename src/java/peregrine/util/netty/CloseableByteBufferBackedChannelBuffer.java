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

import java.io.*;
import java.util.*;
import java.nio.*;
import java.nio.channels.*;

import peregrine.os.*;

/**
 *
 */
public class CloseableByteBufferBackedChannelBuffer extends AbstractChannelBuffer {

    private ByteBufferBackedChannelBuffer delegate = null;
    private MappedFileReader reader = null;

    public CloseableByteBufferBackedChannelBuffer( ByteBufferBackedChannelBuffer delegate,
                                                   MappedFileReader reader ) {

        this.delegate = delegate;
        this.reader = reader;
        
    }

    private void requireOpen() {

        if ( reader.isClosed() )
            throw new RuntimeException( "closed" );
        
    }

    public ChannelBufferFactory factory() {
        requireOpen();
        return delegate.factory();
    }
    
    public boolean isDirect() {
        requireOpen();
        return delegate.isDirect();
    }
        
    public ByteOrder order() {
        requireOpen();
        return delegate.order();
    }
    
    public int capacity() {
        requireOpen();
        return delegate.capacity();
    }
    
    public boolean hasArray() {
        requireOpen();
        return delegate.hasArray();        
    }
    
    public byte[] array() {
        requireOpen();
        return delegate.array();
    }
        
    public int arrayOffset() {
        requireOpen();
        return delegate.arrayOffset();
    }
    
    public byte getByte(int index) {
        requireOpen();
        return delegate.getByte( index );
    }
        
    public short getShort(int index) {
        requireOpen();
        return delegate.getShort( index );
    }
        
    public int getUnsignedMedium(int index) {
        requireOpen();
        return delegate.getUnsignedMedium( index );
    }
        
    public int getInt(int index) {
        requireOpen();
        return delegate.getInt( index );
    }

    public long getLong(int index) {
        requireOpen();
        return delegate.getLong( index );
    }
        
    public void getBytes(int index, ChannelBuffer dst, int dstIndex, int length) {
        requireOpen();
        delegate.getBytes( index, dst, dstIndex, length );
    }
        
    public void getBytes(int index, byte[] dst, int dstIndex, int length) {
        requireOpen();
        delegate.getBytes( index, dst, dstIndex, length );
    }
    
    public void getBytes(int index, ByteBuffer dst) {
        requireOpen();
        delegate.getBytes( index, dst );
    }
        
    public void setByte(int index, int value) {
        requireOpen();
        delegate.setByte( index, value );        
    }
        
    public void setShort(int index, int value) {
        requireOpen();
        delegate.setShort( index, value );
    }
        
    public void setMedium(int index, int   value) {
        requireOpen();
        delegate.setMedium( index, value );
    }
    
    public void setInt(int index, int   value) {
        requireOpen();
        delegate.setInt( index, value );
    }
        
    public void setLong(int index, long  value) {
        requireOpen();
        delegate.setLong( index, value );
    }
        
    public void setBytes(int index, ChannelBuffer src, int srcIndex, int length) {
        requireOpen();
        delegate.setBytes( index, src, srcIndex, length );
    }
    
    public void setBytes(int index, byte[] src, int srcIndex, int length) {
        requireOpen();
        delegate.setBytes( index, src, srcIndex, length );
    }
    
    public void setBytes(int index, ByteBuffer src) {
        requireOpen();
        delegate.setBytes( index, src );
    }
    
    public void getBytes(int index, OutputStream out, int length) throws IOException {
        requireOpen();
        delegate.getBytes( index, out, length );
    }

    public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
        requireOpen();
        return delegate.getBytes( index, out, length );
    }

    public int setBytes(int index, InputStream in, int length) throws IOException {
        requireOpen();
        return delegate.setBytes( index, in, length );
    }

    public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
        requireOpen();
        return delegate.setBytes( index, in, length );
    }

    public ByteBuffer toByteBuffer(int index, int length) {
        throw new RuntimeException( "not supported" );
    }
    
    public ChannelBuffer slice(int index, int length) {
        requireOpen();
        return new CloseableByteBufferBackedChannelBuffer( (ByteBufferBackedChannelBuffer)delegate.slice( index, length ), reader );
    }

    public ChannelBuffer duplicate() {
        requireOpen();
        return new CloseableByteBufferBackedChannelBuffer( (ByteBufferBackedChannelBuffer)delegate.duplicate(), reader );
    }

    public ChannelBuffer copy(int index, int length) {
        requireOpen();
        return new CloseableByteBufferBackedChannelBuffer( (ByteBufferBackedChannelBuffer)delegate.copy( index, length ), reader );
    }

}