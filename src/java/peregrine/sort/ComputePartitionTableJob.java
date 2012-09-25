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
import peregrine.util.*;
import peregrine.reduce.*;
import peregrine.config.*;
import peregrine.io.*;

import com.spinn3r.log5j.*;

/**
 * Map reduce job which computes the partition routing table.
 */
public class ComputePartitionTableJob {

    private static final Logger log = Logger.getLogger();

    public static final int MAX_SAMPLE_SIZE = 100000;

    /**
     * The key length for partition boundaries.  Eight (8) bites for the first
     * component and 8 bytes for the second component.
     *
     * The first component is the value we are sorting by.  Eight bytes give us
     * enough room for doubles/longs.
     *
     * The second component is an 8 byte value for the hashcode for that item.
     * This way we have enough bytes to split data even when the sorting column
     * is the same for 2^64 items.
     * 
     */
    public static final int KEY_LEN = 16;

    public static final StructReader LAST_BOUNDARY  = createByteArray( 127, KEY_LEN );

    private static StructReader createByteArray( int val, int len ) {

        byte[] data = new byte[ len ];

        for( int i = 0; i < data.length; ++i ) {
            data[i] = (byte)val;
        }

        return StructReaders.wrap( data );
        
    }
    
    public static class Map extends Mapper {

        /**
         * The sample data 
         */
        List<StructReader> sample = new ArrayList();

        private JobOutput partitionTable = null;

        @Override
        public void init( Job job, List<JobOutput> output ) {

            super.init( job, output );
            partitionTable = output.get(1);
            
        }

        @Override
        public void map( StructReader key,
                         StructReader value ) {

            key   = StructReaders.wrap( key.toByteArray() );
            value = StructReaders.wrap( value.toByteArray() );
            
            addSample( StructReaders.join( value, key ) );

        }

        /**
         * Add a sample to the 
         */
        public void addSample( StructReader sr ) {

            // we are done sampling.
            if ( sample.size() > MAX_SAMPLE_SIZE )
                return;

            sample.add( sr );

        }
        
        @Override
        public void close() throws IOException {

            //at this point we should have all the samples, sort then and then
            //determine partition boundaries.

            //write out all the key values now.

            log.info( "Sample size is: %,d items", sample.size() );

            if ( sample.size() == 0 ) {
                throw new IOException( "No samples" );
            }
            
            Collections.sort( sample, new StrictStructReaderComparator() );

            //now write out the partitions.

            int nr_partitions = getConfig().getMembership().size();

            int width = sample.size() / nr_partitions;

            int offset = 0;

            log.info( "Going to split across %,d partitions" , nr_partitions );

            List<StructReader> partitionBoundaries = new ArrayList();
            
            long partition_id = 0;

            for( ; partition_id < nr_partitions - 1; ++partition_id ) {

                offset += width;
                partitionBoundaries.add( sample.get( offset - 1 ) );

            }

            partitionBoundaries.add( LAST_BOUNDARY );

            //sort the partitions so that the sort order (asc/desc) of the query
            //is taken into consideration.
            Collections.sort( partitionBoundaries, job.getComparatorInstance() );

            partition_id = 0;

            for( StructReader boundary : partitionBoundaries ) {
                StructReader key = StructReaders.wrap( partition_id );
                emit( key, boundary );
                ++partition_id;
            }
            
        }

        @Override
        public void emit( StructReader key, StructReader value ) {

            log.info( "Going to emit partition table entry: %s=%s" , key.slice().readLong(), Hex.encode( value ) );
            
            partitionTable.emit( key, value );
            
        }
        
    }

    public static class Reduce extends Reducer {

        private static final Logger log = Logger.getLogger();

        private List<StructReader> boundaries = new ArrayList();

        @Override
        public void reduce( StructReader key, List<StructReader> values ) {

            StructReader value = mean( values );
            //StructReader value = median( values );

            log.info( "Going to use final broadcast partition boundary: %s", Hex.encode( value ) );
            boundaries.add( value );

            if ( boundaries.size() == getConfig().getMembership().size() ) {
                emit( key, StructReaders.wrap( boundaries ) );
            }
            
        }

        private StructReader median( List<StructReader> values ) {

            List<StructReader> sorted = new ArrayList( values );
            
            Collections.sort( sorted, new StrictStructReaderComparator() );
            
            return sorted.get( sorted.size() / 2 );
            
        }
        
        private StructReader mean( List<StructReader> values ) {

            int sum = 0;

            byte[] result = new byte[ KEY_LEN ];

            for( int i = 0; i < KEY_LEN; ++i ) {

                for( StructReader current : values ) {
                    sum += current.getByte( i ) & 0xFF;
                }

                result[i] = (byte)(sum / values.size());
                sum = 0;
                
            }

            return StructReaders.wrap( result );

        }

    }

}
