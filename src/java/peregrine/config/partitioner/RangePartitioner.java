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

    private static final int WIDTH = (int)Math.pow( 2, 16 );
    
	private double range;
	
	@Override
    public void init( Config config ) {
		super.init( config );
        init();
    }

	@Override
    public void init( int nr_partitions ) {
        super.init( nr_partitions );
        init();
    }

    public void init() {
    	this.range = WIDTH / (double)nr_partitions;	
    }
    
	@Override
	public Partition partition( StructReader key ) {
		
		byte[] bytes = key.toByteArray();
		
		// the domain of the route function...  basically the key space as an
		// integer so that we can place partitions within that space.

        int value = ((((int) bytes[7]) & 0xFF)     ) +
                    ((((int) bytes[6]) & 0xFF) << 8)
            ;

		int part = (int)(value / range);
			
		return new Partition( part );
		
	}

}
