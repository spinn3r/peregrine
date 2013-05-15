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

package peregrine.reduce.sorter;

import org.jboss.netty.buffer.*;

import peregrine.*;
import peregrine.util.*;

/**
 * Read key/value pairs from a KeyLookup similar to a ChunkReader.
 */
public class KeyLookupReader {

	private KeyLookup lookup = null;
	
    private StructReader key = null;
    
    private StructReader value = null;
	
	public KeyLookupReader(KeyLookup lookup) {
		this.lookup = lookup;
	}

	public boolean hasNext() {
		return lookup.hasNext();
	}
	
	public void next() {

		lookup.next();
		
        KeyEntry current = lookup.get();

        ChannelBuffer backing = current.backing;
        
        int start = current.offset - 1;
        backing.readerIndex( start );

        key   = new StructReader( backing.readSlice( VarintReader.read( backing ) ) );
        value = new StructReader( backing.readSlice( VarintReader.read( backing ) ) );
		
	}
	
	public StructReader key() {
		return key;
	}
	
	public StructReader value() {
		return value;
	}
	
}
