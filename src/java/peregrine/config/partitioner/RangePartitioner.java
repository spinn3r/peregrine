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
package peregrine.config.partitioner;

import peregrine.*;
import peregrine.config.*;

/**
 * Partitions by range based on the key and the partition distribution.
 */
public class RangePartitioner extends BasePartitioner {

	private double range;
	
	@Override
    public void init( Config config ) {
		super.init( config );
    	this.range = 255 / (double)nr_partitions;	
    }
	
	@Override
	public Partition partition( StructReader key ) {
		
		byte[] bytes = key.toByteArray();
		
		// the domain of the route function...  basically the key space as an
		// integer so that we can place partitions within that space.
		int domain = (int)bytes[0] & 0xFF;

		// right now we only support 2^16 or 64k partitions.  I don't think we 
		// will hit this limit any time soon.  This might be famous last words.
		if ( nr_partitions > 255 ) {
			domain = domain << 8;
			domain = domain & ( bytes[1] & 0xFF );
		}
						
		int part = (int)(domain / range);
			
		return new Partition( part );
		
	}

}
