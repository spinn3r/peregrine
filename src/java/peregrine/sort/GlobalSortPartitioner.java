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
package peregrine.sort;

import java.util.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.config.partitioner.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

/**
 * Partitions by range based on the key and the partition distribution.
 */
public class GlobalSortPartitioner extends BasePartitioner {

    private static final Logger log = Logger.getLogger();

    private TreeMap<StructReader,Partition> partitionTable = null;

    public void init( TreeMap<StructReader,Partition> partitionTable ) {
        this.partitionTable = partitionTable;
    }
    
	@Override
	public Partition partition( StructReader key, StructReader value ) {

        // use the sort key of the sort comparator specified. 
        StructReader ptr = job.getComparatorInstance().getSortKey( key, value );
        
        StructReader higherKey = partitionTable.higherKey( ptr );

        if ( higherKey == null )
            throw new RuntimeException( String.format( "No higher key than %s in table %s", Hex.encode( ptr ), partitionTable ) );
        
        Partition result = partitionTable.get( higherKey );

        return result;
        
	}
    
}
