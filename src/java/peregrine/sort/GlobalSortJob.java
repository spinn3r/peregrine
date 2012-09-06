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
import java.io.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.reduce.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

/**
 * Map reduce job which does the full global sort.
 */
public class GlobalSortJob {

    private static final Logger log = Logger.getLogger();
    
    public static class Map extends Mapper {

        @Override
        public void init( Job job, List<JobOutput> output ) {

            super.init( job, output );

            doInitGlobalSortPartitioner( job );
            
        }

        private void doInitGlobalSortPartitioner( Job job ) {

            BroadcastInput partitionTableBroadcastInput = getBroadcastInput().get( 0 );

            List<StructReader> partitionTableEntries
                = StructReaders.unwrap( partitionTableBroadcastInput.getValue() );

            if ( partitionTableEntries.size() == 0 )
                throw new RuntimeException( "No partition table entries" );
            
            log.info( "Working with %,d partition entries", partitionTableEntries.size() );
            
            GlobalSortPartitioner partitioner = (GlobalSortPartitioner)job.getPartitionerInstance();

            TreeMap<StructReader,Partition> partitionTable = new TreeMap( new StrictStructReaderComparator() );

            int partition_id = 0;

            for( StructReader current : partitionTableEntries ) {
                
                log.info( "Adding partition table entry: %s" , Hex.encode( current ) );
                
                partitionTable.put( current, new Partition( partition_id ) );
                ++partition_id;
            }
            
            partitioner.init( partitionTable );

        }
        
    }

    public static class Reduce extends Reducer {

    }

}

