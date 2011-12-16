package peregrine.values;

import java.nio.charset.Charset;

import org.jboss.netty.buffer.*;

import peregrine.util.*;
import peregrine.util.primitive.*;

/**
 * 
 */
public class StructWriter {

    public static int BUFFER_SIZE = 16384;

    protected static Charset UTF8 = null;
    
    private ChannelBuffer buff = null;

    /**
     * StructWriter with max capacity for holding primitive types (8 bytes).
     */
    public StructWriter() {
        this( LongBytes.LENGTH );
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

    public StructWriter writeBytes( byte[] bytes ) {
        writeVarint( bytes.length );
        buff.writeBytes( bytes );
        return this;
    }

    public StructWriter writeString( String value ) {
        writeBytes( value.getBytes( UTF8 ) );
        return this;
    }

    public StructWriter writeHashcode( int value ) {
        return writeHashcode( (long)value );
    }

    public StructWriter writeHashcode( long value ) {
        return writeHashcode( LongBytes.toByteArray( value ) );
    }

    public StructWriter writeHashcode( byte[] value ) {
        // our hash codes right now are fixed width.
        buff.writeBytes( Hashcode.getHashcode( value ) );
        return this;
    }
        
    public StructWriter writeHashcode( String value ) {
        // our hash codes right now are fixed width.
        buff.writeBytes( Hashcode.getHashcode( value ) );
        return this;
    }

    public ChannelBuffer getChannelBuffer() {
        return buff.duplicate();
    }

    public StructReader toStructReader() {
        return new StructReader( getChannelBuffer() );
    }
    
    public byte[] toBytes() {

        int len = buff.writerIndex();
        byte[] result = new byte[ len ];

        buff.readBytes( result );
        
        return result;
        
    }

    static {
        UTF8 = Charset.forName( "UTF-8" );
    }

}
