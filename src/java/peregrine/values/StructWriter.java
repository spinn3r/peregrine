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

    private static Charset UTF8 = null;
    
    private ChannelBuffer buff = null;

    private static ThreadLocalChannelBuffer threadLocal =
        new ThreadLocalChannelBuffer( BUFFER_SIZE );

    public StructWriter() {
        //TODO: I should probably make this an extent based buffer so that we
        //don't accidentally blow up in the future.  Also, this need to be
        //thread local but there was a RARE race conditition that I need to
        //track down.
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
    	//buff.writeInt( value );
    	buff.writeBytes( IntBytes.toByteArray(value) );
    	return this;
    }
    
    public StructWriter writeDouble( double value ) {
    	// FIXME why isn't this using writeDouble?
        buff.writeBytes( LongBytes.toByteArray( Double.doubleToLongBits( value ) ) );
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

        byte[] backing = buff.array();
        int len = buff.writerIndex();
        
        byte[] result = new byte[ len ];
        System.arraycopy( backing, 0, result, 0, len );
        
        return result;
        
    }

    static {
        UTF8 = Charset.forName( "UTF-8" );
    }

}
