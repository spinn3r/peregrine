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
package peregrine.globalsort;

import java.util.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.config.partitioner.*;

import com.spinn3r.log5j.*;

/**
 * Partitions by range based on the key and the partition distribution.
 */
public class GlobalSortPartitioner extends BasePartitioner {

    private static final Logger log = Logger.getLogger();

	private int records = -1;

    private int index = 0;

    private int width;

    private int partition = -1;

    private TreeMap<StructReader,Partition> partitionIndex = null;
    
	@Override
    public void init( LocalContext localContext ) {

		super.init( localContext );
        
        records = localContext.getRecords();

        width = (int)(records / nr_partitions);

        partition = localContext.getPartition().getId();
        
    }

    public void init( TreeMap<StructReader,Partition> partitionIndex ) {
        this.partitionIndex = partitionIndex;
    }
    
	@Override
	public Partition partition( StructReader key, StructReader value ) {

        StructReader ptr = StructReaders.join( value.readSlice( 8 ), key );

        StructReader higherKey = partitionIndex.higherKey( ptr );
        
        Partition result = partitionIndex.get( higherKey );

        return result;
        
	}
    
}
