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

import peregrine.*;
import peregrine.util.netty.*;
import peregrine.util.primitive.*;
import peregrine.io.chunk.*;

public class KeyEntry implements KeyValuePair {

	protected byte bufferIndex;

	protected int offset;

    protected ChannelBuffer backing = null; 

    protected DefaultChunkReader reader = null;
    
    protected int valueOffset = -1;

    protected StructReader key = null;

    protected StructReader value = null;

	public KeyEntry( byte bufferIndex, int offset, ChannelBuffer backing ) {
		this.bufferIndex = bufferIndex;
		this.offset = offset;
        this.backing = backing;

        if ( bufferIndex < 0 )
            throw new RuntimeException( "Invalid buffer index: " + bufferIndex );
        
	}

	public KeyEntry( byte bufferIndex, int offset, ChannelBuffer backing, DefaultChunkReader reader ) {

        this( bufferIndex, offset, backing );
        this.reader = reader;
        readKey();
        
	}

    /**
     * Read a byte from the entry at the given position within the key. 
     * 
     */
    public byte read( int pos ) {    	
        return backing.getByte( offset + pos );
    }

    public void readKey() {

        backing.readerIndex( offset - 1 );

        this.key = reader.readEntry();
        this.valueOffset = backing.readerIndex() + 1;
        
    }

    public void readValue() {

        backing.readerIndex( valueOffset - 1 );

        this.value = reader.readEntry();

    }
    
    public StructReader getKey() { 
        return this.key;
    }

    public StructReader getValue() { 

        if ( value == null ) {
            readValue();
        }
            
        return this.value;

    }

}

