package peregrine.values;

import org.jboss.netty.buffer.*;

import peregrine.util.*;
import peregrine.util.primitive.*;
import peregrine.os.*;

/**
 * API for dealing with complex data structures as byte array.  All main byte
 * manipulation in peregrine is centered around StructReader and StructWriter.
 * A sibling class StructReaders provides API around easily accessing
 * primitives.
 * 
 * @see StructWriter
 * @see StructReaders
 */
public class StructReader {

    private VarintReader varintReader;
    private ChannelBuffer buff;
    private MappedFile backing = null;
    
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

    public StructReader( ChannelBuffer buff, MappedFile backing ) {
        this( buff );
        this.backing = backing;
    }

    public byte readByte() {
        requireOpen();

        return buff.readByte();
    }

    public short readShort() {
        requireOpen();

        return buff.readShort();
    }

    public int readVarint() {
        requireOpen();

        return varintReader.read();
    }

    public int readInt() {
        requireOpen();

        return buff.readInt();
    }

    public long readLong() {
        requireOpen();

        return buff.readLong();
    }

    public float readFloat() {
        requireOpen();

        return buff.readFloat();
    }

    public double readDouble() {
        requireOpen();

        return buff.readDouble();
    }

    public boolean readBoolean() {
        requireOpen();

    	return buff.readByte() == 1;
    }
    
    public char readChar() {
        requireOpen();

        return buff.readChar();
    }

    public byte[] readBytes() {
        requireOpen();

        int len = readVarint();
        byte[] data = new byte[ len ];
        buff.readBytes( data );
        return data;
    }

    public String readString() {
        requireOpen();

        byte[] data = readBytes();
        return new String( data, StructWriter.UTF8 );
    }
    
    public byte[] read( byte[] data ) {
        requireOpen();

        buff.readBytes( data );
        return data;
    }

    public byte[] readHashcode() {
        requireOpen();

        return read(new byte[Hashcode.HASH_WIDTH]);
    }

    public byte[] toByteArray() {

        requireOpen();

        byte[] result = new byte[ length() ];
        buff.getBytes( 0, result, 0, result.length );

        return result;
    }
    
    public ChannelBuffer getChannelBuffer() {
    	return buff;	
    }
    
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

    private void requireOpen() {

        if ( backing != null && backing.isClosed() )
            throw new RuntimeException( "closed" );
        
    }
    
}

