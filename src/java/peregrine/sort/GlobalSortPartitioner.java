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
import peregrine.io.*;

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

    public void init( Job job, BroadcastInput partitionTableBroadcastInput ) {

        List<StructReader> partitionTableEntries
            = StructReaders.unwrap( partitionTableBroadcastInput.getValue() );
        
        if ( partitionTableEntries.size() == 0 )
            throw new RuntimeException( "No partition table entries" );
        
        log.info( "Working with %,d partition entries", partitionTableEntries.size() );
        
        TreeMap<StructReader,Partition> partitionTable = new TreeMap( job.getComparatorInstance() );
        
        int partition_id = 0;
        
        for( StructReader current : partitionTableEntries ) {
            
            log.info( "Adding partition table entry: %s" , Hex.encode( current ) );
            
            partitionTable.put( current, new Partition( partition_id ) );
            ++partition_id;
        }
        
        init( partitionTable );

    }
    
	@Override
	public Partition partition( StructReader key, StructReader value ) {

        // use the sort key of the sort comparator specified. 
        StructReader ptr = job.getComparatorInstance().getSortKey( key, value );

        // NOTE: the way we build the partition table means that there will
        // always be a key with a higher value. 
        Partition result = partitionTable.ceilingEntry( ptr ).getValue();

        return result;
        
	}
    
}
