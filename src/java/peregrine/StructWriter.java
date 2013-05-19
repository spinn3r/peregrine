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
package peregrine;

import java.nio.*;
import java.nio.charset.Charset;

import org.jboss.netty.buffer.*;

import peregrine.util.*;


/**
 * <p>
 * Write data to a struct and convert it to a StructReader when done.  This is
 * generally used for more complicated data structures consisting of multiple
 * values / types strung together.
 * 
 * <p>Also see StructReaders for a simbling class which provides easy factory
 * methods for creating StructReaders instead of always having to use
 * StructWriter.
 */
public class StructWriter {

    public static int BUFFER_SIZE = 16384;

    private ChannelBuffer buff = null;

    /**
     * StructWriter with max capacity for holding primitive types (8 bytes).
     */
    public StructWriter() {
        this( Longs.LENGTH );
    }

    /**
     * StructWriter for a raw ChannelBuffer.
     */
    public StructWriter( ChannelBuffer buff ) {
    	this.buff = buff;
    }

    /**
     * With a specific capacity.
     */
    public StructWriter( int capacity ) {
    	this( ChannelBuffers.buffer( capacity ) );
    }

    public StructWriter writeByte( byte value ) {
    	buff.writeByte( value );
    	return this;
    }

    public StructWriter writeShort( short value ) {
    	buff.writeShort( value );
    	return this;
    }

    public StructWriter writeVarint( int value ) {
        VarintWriter.write( buff, value );
        return this;
    }

    public StructWriter writeInt( int value ) {
    	buff.writeInt( value );
    	return this;
    }

    public StructWriter writeLong( long value ) {
    	buff.writeLong(value);
    	return this;
    }

    public StructWriter writeFloat( float value ) {
    	buff.writeFloat(value);
        return this;
    }

    public StructWriter writeDouble( double value ) {
    	buff.writeDouble(value);
        return this;
    }

    public StructWriter writeBoolean( boolean value ) {
    	
    	if ( value )
    	    buff.writeByte((byte)1);
    	else 
    	    buff.writeByte((byte)0);

        return this;
        
    }
    
    public StructWriter writeChar( char value ) {
    	buff.writeChar(value);
        return this;
    }

    /**
     * Write a length prefixed byte array to this struct.  Call
     * {@link StructReader#readBytes} to read it back out.  The length of the
     * array will be preserved.
     */
    public StructWriter writeBytes( byte[] bytes ) {
        writeVarint( bytes.length );
        writeBytesFixed( bytes );
        return this;
    }

    /**
     * Write a fixed length byte array to the struct.  The length is NOT
     * included so you will need to keep track of the lenght after the fact.
     * 
     * <p>
     * One can call {@link StructReader#readBytesFixed(int)} and specify the
     * number of bytes you want to read.
     */
    public StructWriter writeBytesFixed( byte[] bytes ) {
        buff.writeBytes( bytes );
        return this;
    }

    public StructWriter writeString( String value ) {
        writeBytes( value.getBytes( Charsets.UTF8 ) );
        return this;
    }

    public StructWriter writeHashcode( int value ) {
        return writeHashcode( (long)value );
    }

    public StructWriter writeHashcode( long value ) {
        return writeHashcode( Longs.toByteArray( value ) );
    }

    public StructWriter writeHashcode( byte[] value ) {
        // our hash codes right now are fixed width.
        writeBytesFixed( Hashcode.getHashcode( value ) );
        return this;
    }
        
    public StructWriter writeHashcode( String value ) {
        // our hash codes right now are fixed width.
        writeBytesFixed( Hashcode.getHashcode( value ) );
        return this;
    }

    public ChannelBuffer getChannelBuffer() {
        return buff.duplicate();
    }

    public StructReader toStructReader() {
        return new StructReader( getChannelBuffer() );
    }

}
