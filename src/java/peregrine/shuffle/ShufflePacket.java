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
package peregrine.shuffle;

import org.jboss.netty.buffer.*;

public class ShufflePacket {

    public int from_partition;
    public int from_chunk;
    public int to_partition;

    /**
     * The byte offset of this file within its parent.
     */
    public int offset;
    public int count;
    public ChannelBuffer data; 

    public ShufflePacket( int from_partition,
                          int from_chunk,
                          int to_partition,
                          int offset,
                          int count,
                          ChannelBuffer data ) {
        
        this.from_partition = from_partition;
        this.from_chunk = from_chunk;
        this.to_partition = to_partition;
        this.offset = offset;
        this.count = count;
        this.data = data;

    }

    public String toString() {

        return String.format( "from_partition: %s, from_chunk: %s, to_partition: %s, offset; %,d, count: %,d, length: %,d bytes",
                              from_partition,
                              from_chunk,
                              to_partition,
                              offset,
                              count,
                              data.capacity() );

    }

    public int getOffset() {
        return offset;
    }                            
    
}
