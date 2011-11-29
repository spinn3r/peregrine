package peregrine.values;

import org.jboss.netty.buffer.*;

import peregrine.util.*;
import peregrine.util.primitive.*;

/**
 * 
 */
public class StructReader {

    private VarintReader varintReader;
    private ChannelBuffer buff;
    
    public StructReader( byte[] data ) {
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

    public byte[] readBytes() {
        int len = readVarint();
        byte[] data = new byte[ len ];
        buff.readBytes( data );
        return data;
    }

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

    public byte[] toByteArray() {
        byte[] result = new byte[ buff.writerIndex() ];
        buff.getBytes( 0, result, 0, result.length );
        return result;
    }
    
    public ChannelBuffer getChannelBuffer() {
    	return buff;	
    }
    
    public int length() {
    	return buff.writerIndex();
    }
    
}

