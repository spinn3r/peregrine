/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package peregrine.util.netty;

import org.jboss.netty.buffer.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

/**
 * A dynamic capacity buffer which increases its capacity as needed.  It is
 * recommended to use {@link ChannelBuffers#dynamicBuffer(int)} instead of
 * calling the constructor explicitly.
 *
 * @author <a href="http://www.jboss.org/netty/">The Netty Project</a>
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 *
 * @version $Rev: 2206M $, $Date: 2010-11-09 15:04:10 +0900 (Tue, 09 Nov 2010) $
 *
 */
public class SlabDynamicChannelBuffer extends AbstractChannelBuffer {

    private final ChannelBufferFactory factory;
    private final ByteOrder endianness;
    private ChannelBuffer buffer;

    private List<ChannelBuffer> extents = new ArrayList();

    private int extentLength;

    private int capacity = 0;
    
    public SlabDynamicChannelBuffer(int initialCapacity, int extentLength) {
        this(ByteOrder.BIG_ENDIAN, initialCapacity, extentLength);
    }

    public SlabDynamicChannelBuffer(ByteOrder endianness, int initialCapacity, int extentLength) {
        this(endianness, initialCapacity, extentLength, HeapChannelBufferFactory.getInstance(endianness));
    }

    public SlabDynamicChannelBuffer(ByteOrder endianness, int initialCapacity, int extentLength, ChannelBufferFactory factory) {
        if (extentLength < 0) {
            throw new IllegalArgumentException("extentLength: " + extentLength);
        }
        if (endianness == null) {
            throw new NullPointerException("endianness");
        }
        if (factory == null) {
            throw new NullPointerException("factory");
        }
        this.factory = factory;
        this.endianness = endianness;
        this.extentLength = extentLength;

        extend( initialCapacity );
        
    }

    @Override
    public void ensureWritableBytes(int minWritableBytes) {
        
        int writableBytes = capacity - writerIndex();
        if ( minWritableBytes <= writableBytes) {
            return;
        }

        int extentLength = this.extentLength;

        if ( extentLength < minWritableBytes ) {
            extentLength = minWritableBytes;
        }
        
        extend( extentLength );

    }

    private void extend( int extentLength ) {

        ChannelBuffer newExtent = factory().getBuffer(order(), extentLength );

        extents.add( newExtent );
        
        buffer = new CompositeChannelBuffer( endianness, extents, true );

        capacity += extentLength;
        
    }
    
    public ChannelBufferFactory factory() {
        return factory;
    }

    public ByteOrder order() {
        return endianness;
    }

    public boolean isDirect() {
        return buffer.isDirect();
    }

    public int capacity() {
        return buffer.capacity();
    }

    public boolean hasArray() {
        return buffer.hasArray();
    }

    public byte[] array() {
        return buffer.array();
    }

    public int arrayOffset() {
        return buffer.arrayOffset();
    }

    public byte getByte(int index) {
        return buffer.getByte(index);
    }

    public short getShort(int index) {
        return buffer.getShort(index);
    }

    public int getUnsignedMedium(int index) {
        return buffer.getUnsignedMedium(index);
    }

    public int getInt(int index) {
        return buffer.getInt(index);
    }

    public long getLong(int index) {
        return buffer.getLong(index);
    }

    public void getBytes(int index, byte[] dst, int dstIndex, int length) {
        buffer.getBytes(index, dst, dstIndex, length);
    }

    public void getBytes(int index, ChannelBuffer dst, int dstIndex, int length) {
        buffer.getBytes(index, dst, dstIndex, length);
    }

    public void getBytes(int index, ByteBuffer dst) {
        buffer.getBytes(index, dst);
    }

    public int getBytes(int index, GatheringByteChannel out, int length)
            throws IOException {
        return buffer.getBytes(index, out, length);
    }

    public void getBytes(int index, OutputStream out, int length)
            throws IOException {
        buffer.getBytes(index, out, length);
    }

    public void setByte(int index, int value) {
        buffer.setByte(index, value);
    }

    public void setShort(int index, int value) {
        buffer.setShort(index, value);
    }

    public void setMedium(int index, int value) {
        buffer.setMedium(index, value);
    }

    public void setInt(int index, int value) {
        buffer.setInt(index, value);
    }

    public void setLong(int index, long value) {
        buffer.setLong(index, value);
    }

    public void setBytes(int index, byte[] src, int srcIndex, int length) {
        buffer.setBytes(index, src, srcIndex, length);
    }

    public void setBytes(int index, ChannelBuffer src, int srcIndex, int length) {
        buffer.setBytes(index, src, srcIndex, length);
    }

    public void setBytes(int index, ByteBuffer src) {
        buffer.setBytes(index, src);
    }

    public int setBytes(int index, InputStream in, int length)
            throws IOException {
        return buffer.setBytes(index, in, length);
    }

    public int setBytes(int index, ScatteringByteChannel in, int length)
            throws IOException {
        return buffer.setBytes(index, in, length);
    }

    @Override
    public void writeByte(int value) {
        ensureWritableBytes(1);
        super.writeByte(value);
    }

    @Override
    public void writeShort(int value) {
        ensureWritableBytes(2);
        super.writeShort(value);
    }

    @Override
    public void writeMedium(int value) {
        ensureWritableBytes(3);
        super.writeMedium(value);
    }

    @Override
    public void writeInt(int value) {
        ensureWritableBytes(4);
        super.writeInt(value);
    }

    @Override
    public void writeLong(long value) {
        ensureWritableBytes(8);
        super.writeLong(value);
    }

    @Override
    public void writeBytes(byte[] src, int srcIndex, int length) {
        ensureWritableBytes(length);
        super.writeBytes(src, srcIndex, length);
    }

    @Override
    public void writeBytes(ChannelBuffer src, int srcIndex, int length) {
        ensureWritableBytes(length);
        super.writeBytes(src, srcIndex, length);
    }

    @Override
    public void writeBytes(ByteBuffer src) {
        ensureWritableBytes(src.remaining());
        super.writeBytes(src);
    }

    @Override
    public int writeBytes(InputStream in, int length) throws IOException {
        ensureWritableBytes(length);
        return super.writeBytes(in, length);
    }

    @Override
    public int writeBytes(ScatteringByteChannel in, int length)
            throws IOException {
        ensureWritableBytes(length);
        return super.writeBytes(in, length);
    }

    @Override
    public void writeZero(int length) {
        ensureWritableBytes(length);
        super.writeZero(length);
    }

    public ChannelBuffer duplicate() {
        return new DuplicatedChannelBuffer(this);
    }

    public ChannelBuffer copy(int index, int length) {
        SlabDynamicChannelBuffer copiedBuffer = new SlabDynamicChannelBuffer(order(), Math.max(length, 64), extentLength, factory());
        copiedBuffer.buffer = buffer.copy(index, length);
        copiedBuffer.setIndex(0, length);
        return copiedBuffer;
    }

    public ChannelBuffer slice(int index, int length) {
        if (index == 0) {
            if (length == 0) {
                return ChannelBuffers.EMPTY_BUFFER;
            }
            return new TruncatedChannelBuffer(this, length);
        } else {
            if (length == 0) {
                return ChannelBuffers.EMPTY_BUFFER;
            }
            return new SlicedChannelBuffer(this, index, length);
        }
    }

    public ByteBuffer toByteBuffer(int index, int length) {
        return buffer.toByteBuffer(index, length);
    }
}
