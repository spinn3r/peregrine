/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.util;

import peregrine.util.netty.*;

import org.jboss.netty.buffer.*;

/**
 * A thread local ChannelBuffer which allows us to write on a buffer and reset
 * it every time we use it which is much faster than continually re-allocating.
 */
public class ThreadLocalChannelBuffer extends ThreadLocal<ChannelBuffer> {

    private int capacity;
    
    public ThreadLocalChannelBuffer( int capacity ) {
        this.capacity = capacity;
    }

    @Override
    public ChannelBuffer get() {

        ChannelBuffer result = super.get();

        // reset the previous one... 
        result.readerIndex( 0 );
        result.writerIndex( 0 );

        return result;
        
    }
    
    @Override
    public ChannelBuffer initialValue() {

        if ( capacity <= 0 )
            throw new IllegalArgumentException( "capacity" );

        // support extension if we need it...
        return new SlabDynamicChannelBuffer( capacity, capacity );
        
    }
    
}

