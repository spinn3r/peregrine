package peregrine.values;

import java.nio.charset.Charset;

import org.jboss.netty.buffer.*;

import peregrine.util.*;

/**
 * 
 */
public class StructWriter {

    public static int BUFFER_SIZE = 16384;

    private static Charset UTF8 = null;
    
    private ChannelBuffer buff = null;

    private static ThreadLocalChannelBuffer threadLocal =
        new ThreadLocalChannelBuffer( BUFFER_SIZE );

    public StructWriter() {
        this( threadLocal.get() );
    }

    public StructWriter( ChannelBuffer buff ) {
    	this.buff = buff;
    }

    public StructWriter( int capacity ) {
    	this( ChannelBuffers.buffer( capacity ) );
    }
    
    public StructWriter writeVarint( int value ) {
        VarintWriter.write( buff, value );
        return this;
    }

    public StructWriter writeInt( int value ) {
    	buff.writeInt( value );
    	return this;
    }

    public StructWriter writeLong( int value ) {
    	buff.writeLong(value);
    	return this;
    }
    
    public StructWriter writeDouble( double value ) {
    	buff.writeDouble(value);
        return this;
    }
    
    public StructWriter writeHashcode( String key ) {
        buff.writeBytes( Hashcode.getHashcode( key ) );
        return this;
        
    }

    public StructWriter writeString( String value ) {
        buff.writeBytes( value.getBytes( UTF8 ) );
        return this;
    }
    
    public ChannelBuffer getChannelBuffer() {
    	return buff;
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
