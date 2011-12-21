package peregrine.util.netty;

import java.nio.*;

import org.jboss.netty.buffer.*;

public class DirectChannelBufferFactory extends AbstractChannelBufferFactory {

	@Override
	public ChannelBuffer getBuffer(ByteOrder endianness, int capacity) {

        try {
            return ChannelBuffers.directBuffer( endianness, capacity );
        } catch ( OutOfMemoryError e ) {

            // try to GC at LEAST once so that we can attempt to free up memory
            // used by buffers which haven't yet been reclaimed. If this fails
            // then go ahead and throw an OutOfMemoryError ...
            System.gc();

            return ChannelBuffers.directBuffer( endianness, capacity );
            
        }

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
