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
package peregrine;

import java.nio.*;

import org.jboss.netty.buffer.*;

import peregrine.util.*;
import peregrine.util.primitive.*;
import peregrine.os.*;

/**
 * API for dealing with complex data structures as high level types.  All main
 * byte manipulation in peregrine is centered around StructReader and
 * StructWriter.  A sibling class {@link StructReaders} provides API around easily
 * accessing primitives.
 * 
 * <p> We support the following types:
 *
 * <pre>
 *
 * byte
 * short
 * varint
 * int
 * long
 * float
 * double
 * boolean
 * char
 * string
 * hashcode
 * 
 * </pre>
 * 
 * @see StructWriter
 * @see StructReaders
 */
public class StructReader {

    private VarintReader varintReader;

    protected ChannelBuffer buff;
    
    public StructReader( byte[] data ) {
        this( ChannelBuffers.wrappedBuffer( data ) );
    }

    public StructReader( ByteBuffer data ) {
        this( ChannelBuffers.wrappedBuffer( data ) );
    }

    public StructReader( int capacity ) {
        this( ChannelBuffers.buffer( capacity ) );
    }
    
    public StructReader( ChannelBuffer buff ) {
    	this.buff = buff;
        this.varintReader = new VarintReader( buff );
    }

    public byte readByte() {

        return buff.readByte();
    }

    public short readShort() {

        return buff.readShort();
    }

    public int readVarint() {

        return varintReader.read();
    }

    public int readInt() {

        return buff.readInt();
    }

    public long readLong() {

        return buff.readLong();
    }

    public float readFloat() {

        return buff.readFloat();
    }

    public double readDouble() {

        return buff.readDouble();
    }

    public boolean readBoolean() {

    	return buff.readByte() == 1;
    }
    
    public char readChar() {

        return buff.readChar();
    }

    /**
     * Read a varint prefixed slice from this StructReader.
     */
    public StructReader readSlice() {
        return readStruct( readVarint() );
    }
    
    /**
     * Read a slice of bytes from this struct reader and return another
     * StructReader.
     */
    public StructReader readSlice( int length ) {
        return readStruct( length );
    }

    /**
     * Read a new struct from the current struct from the given lenght bytes.
     */
    public StructReader readStruct( int length ) {
        return new StructReader( buff.readSlice( length ) );
    }
    
    /**
     * Read a byte array and return it.  The byte byte array is length prefixed
     * so that this StructReader can hold mulitiple byte arrays.
     */
    public byte[] readBytes() {
        return readBytesFixed( readVarint() );
    }

    /**
     * Read a fixed width byte array from the stream.  The size must be known
     * ahead of time.  This can be useful for writing a large number of objects
     * like longs, hashcodes (8 bytes), etc which are all fixed without having
     * to store the length prefix which would yield overhead.
     */
    public byte[] readBytesFixed( int size ) {
        byte[] data = new byte[ size ];
        buff.readBytes( data );
        return data;
    }
    
    /**
     * Read a length prefixed string UTF8 string from the stream.
     */
    public String readString() {

        byte[] data = readBytes();
        return new String( data, StructWriter.UTF8 );
        
    }
    
    public byte[] read( byte[] data ) {

        buff.readBytes( data );
        return data;
    }

    public byte[] readHashcode() {

        return read(new byte[Hashcode.HASH_WIDTH]);
    }

    public String readHashcodeAsBase64() {
        return Base64.encode( readHashcode() );
    }
    
    /**
     * Read all the data in this StructReader as a byte array.  Useful if you
     * have some alternative form of data representation you with to work with
     * and need the bytes directly.
     */
    public byte[] toByteArray() {

        byte[] result = new byte[ length() ];
        buff.getBytes( 0, result, 0, result.length );

        return result;
    }

    /**
     * Get the ChannelBuffer that backs this StructReader.
     */
    public ChannelBuffer getChannelBuffer() {
    	return buff;	
    }

    /**
     * Get this StructReader as a ByteBuffer
     */
    public ByteBuffer toByteBuffer() {
        return getChannelBuffer().toByteBuffer();
    }
    
    /**
     * Return the length of the number of bytes written.
     */
    public int length() {
    	return buff.writerIndex();
    }

    /**
     * Reset for reading again.  This positions the pointer at the beginning of
     * the struct.
     */
    public void reset() {
        buff.readerIndex( 0 );
    }

    /**
     * Get a byte at an absolute position.  This is used so that we can do
     * sorting based on values or keys.
     */
    public void getByte( int index ) {
        buff.getByte( index );
    }
    
    /**
     * <p>
     * Return true if the struct is currently readable and there is data ready
     * to be returned.  Every read call increments the readerIndex and once
     * readerIndex = writerIndex then this StructReader is no longer readable.
     *
     * <p>
     * This can be used in loops where you want to keep reading from the
     * StructReader like an enumeration.
     */
    public boolean isReadable() {
        return buff.readerIndex() < buff.writerIndex();
    }

}

