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
package peregrine.reduce.sorter;

import org.jboss.netty.buffer.*;

import peregrine.util.primitive.*;

public class KeyEntry {

	public byte bufferIndex;
	public int offset;
    public ChannelBuffer backing; 
	
	public KeyEntry( byte bufferIndex, int offset, ChannelBuffer backing ) {
		this.bufferIndex = bufferIndex;
		this.offset = offset;
        this.backing = backing;
	}

    /*
    public byte[] read() {
        byte[] data = new byte[LongBytes.LENGTH];
        backing.getBytes( offset, data );
        return data;
    }

    */
    
    /**
     * Read a byte from the entry at the given position within the key. 
     * 
     */
    public byte read( int pos ) {    	
        return backing.getByte( offset + pos );
    }
    
}
