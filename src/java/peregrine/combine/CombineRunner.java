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
package peregrine.combine;

import java.util.*;
import java.io.*;

import peregrine.*;
import peregrine.reduce.*;
import peregrine.reduce.sorter.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.util.*;
import peregrine.util.primitive.*;

/**
 * Run a combine on the given ChunkReader and use the given combiner to
 * merge/compress/collapse the results before we send them over the wire. 
 */
public class CombineRunner {

	private Config config;
	
	public CombineRunner(Config config) {
		this.config = config;
	}
	
    /**
     * Take the given reader and combine records.
     */
    public void combine( ChunkReader reader, Combiner combiner ) throws IOException {

        merge( sort( reader ), combiner );
    	
    }

    private void merge( KeyLookupReader reader, Combiner combiner ) throws IOException {

        byte[] last = null;
        List<StructReader> values = new ArrayList();

        FullKeyComparator comparator = new FullKeyComparator();
        
        if ( reader.hasNext() ) {

        	reader.next();
        	
            StructReader key = reader.key();
            StructReader value = reader.value();

            if ( comparator.compare( key.toByteArray() , last ) == 0 ) {

                values.add( value );
                
            } else {

                combiner.combine( key , values );
                
                last = key.toByteArray();
                values = new ArrayList();
                
            }

        }

    }
    
    private KeyLookupReader sort( ChunkReader reader ) throws IOException {

    	ChunkSorter sorter = new ChunkSorter();
    	
    	KeyLookup lookup = new KeyLookup( new CompositeChunkReader( config, reader ) );
    	KeyLookup sorted = sorter.sort( lookup );
    	
        return new KeyLookupReader( sorted );
        
    }
    
}
