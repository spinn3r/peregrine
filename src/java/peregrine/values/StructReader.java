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

    /**
     * Read a byte array and return it.  The byte byte array is length prefixed
     * so that this StructReader can hold mulitiple byte arrays.
     */
    public byte[] readBytes() {
        requireOpen();
        return readBytesFixed( readVarint() );
    }

    /**
     * Read a fixed width byte array from the stream.  The size must be known
     * ahead of time.  This can be useful for writing a large number of objects
     * like longs, hashcodes (8 bytes), etc which are all fixed without having
     * to store the length prefix which would yield overhead.
     */
    public byte[] readBytesFixed( int size ) {
        requireOpen();
        byte[] data = new byte[ size ];
        buff.readBytes( data );
        return data;
    }

    /**
     * Read a length prefixed string.
     */
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

    private void requireOpen() {

        if ( backing != null && backing.isClosed() )
            throw new RuntimeException( "closed" );
        
    }
    
}

