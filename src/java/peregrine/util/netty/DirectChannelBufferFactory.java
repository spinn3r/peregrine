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
package peregrine.util.netty;

import java.nio.*;

import org.jboss.netty.buffer.*;

public class DirectChannelBufferFactory extends AbstractChannelBufferFactory {
    
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
