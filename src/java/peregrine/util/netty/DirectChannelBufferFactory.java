package peregrine.util.netty;

import java.nio.*;

import org.jboss.netty.buffer.*;

public class DirectChannelBufferFactory extends AbstractChannelBufferFactory {

    /**
     * True if we should require that the GC be run before attempting another
     * allocation.
     */
    public volatile boolean requireGC = false;
    
	@Override
	public ChannelBuffer getBuffer(ByteOrder endianness, int capacity) {

        return ChannelBuffers.directBuffer( endianness, capacity );

	}

	@Override
	public ChannelBuffer getBuffer( ByteOrder endianness, 
									byte[] array,
									int offset, 
									int length) {
		
		throw new RuntimeException( "Unable to create direct buffer from array.  Would not be direct." );
	}

	@Override
	public ChannelBuffer getBuffer(ByteBuffer nioBuffer) {
		
        if ( ! nioBuffer.isDirect() )
    		throw new RuntimeException( "Unable to create direct buffer from array.  Would not be direct." );
        
		return ChannelBuffers.wrappedBuffer( nioBuffer );
			
	}
	
}
