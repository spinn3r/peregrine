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

import java.math.*;
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

    /**
     * The maximum number of keys to read into memory to compute the sample.
     */
    public static final int MAX_SAMPLE_SIZE = 100; //FIXME set back to 100k

    /**
     * <p>
     * The key length for partition boundaries.  Eight (8) bites for the first
     * component and 8 bytes for the second component.
     *
     * <p>
     * The first component is the value we are sorting by.  Eight bytes give us
     * enough room for doubles/longs.
     *
     * <p>
     * The second component is an 8 byte value for the hashcode for that item.
     * This way we have enough bytes to split data even when the sorting column
     * is the same for 2^64 items.
     * 
     * <p>In the future we should consider a way to have custom width sort keys.
     */
    public static final int KEY_LEN = 16;

    public static final StructReader LAST_BOUNDARY  = createByteArray( 127, KEY_LEN );
    
    /**
     * Create a byte array of the given values.
     */
    private static StructReader createByteArray( int val, int len ) {

        byte[] data = new byte[ len ];

        for( int i = 0; i < data.length; ++i ) {
            data[i] = (byte)val;
        }

        return StructReaders.wrap( data );
        
    }

    public static List<StructReader> computePartitionBoundaries( Config config,
                                                                 Partition part,
                                                                 SortComparator comparator,
                                                                 List<StructReader> sample ) {

        log.info( "Sample size is: %,d items", sample.size() );

        List<StructReader> result = new ArrayList();

        if ( sample.size() == 0 ) {

            // It is totally reasonable to want to sort a file with no
            // entries.
            
            log.warn( "No samples for job." );

            return result;
            
        }

        //Collections.sort( sample, comparator );
        Collections.sort( sample, new StrictStructReaderComparator() );
        
        log( "samples for: " + part, sample );
        
        //now write out the partitions.

        int nr_partitions = config.getMembership().size();

        int width = sample.size() / nr_partitions;

        int offset = 0;

        log.info( "Going to split across %,d partitions" , nr_partitions );

        long partition_id = 0;

        for( ; partition_id < nr_partitions - 1; ++partition_id ) {

            offset += width - 1;

            StructReader sr = sample.get( offset );

            log.info( "FIXME: Took sample %s on partition %s at offset=%s with sample size=%s\n", sr.toInteger(), part, offset, sample.size() );
            
            result.add( sr );

        }

        result.add( LAST_BOUNDARY );

        return result;

    }

    public static String format( StructReader sr ) {
        return String.format( "%s=%s", sr.toInteger(), sr.toString() );
    }

    public static void log( String description, List<StructReader> list ) {

        StringBuilder buff = new StringBuilder();

        buff.append( "\n" );

        int i = 0;
        for( StructReader sr : list ) {
            buff.append( String.format( "FIXME: dump: %04d/%04d=%s: %s\n" , i+1, list.size(), description, format( sr ) ) );
            ++i;
        }

        log.info( "%s", buff.toString() );
        
    }

    public static void dump( String description, List<StructReader> list ) {

        for( StructReader sr : list ) {
            System.out.printf( "FIXME: dump: %s: %s\n" , description, format( sr ) );
        }

    }

    public static class Map extends Mapper {

        private static final Logger log = Logger.getLogger();

        /**
         * The sample data.  We sort this list and then take samples at
         * partition boundaries.
         */
        private List<StructReader> sample = new ArrayList();

        private SortComparator comparator = null;
        
        @Override
        public void init( Job job, List<JobOutput> output ) {

            super.init( job, output );

            log.info( "FIXME last boundary: %s" , LAST_BOUNDARY.toInteger() );
            
            try {

                Class clazz = job.getParameters().getClass( "sortComparator" );
                comparator = (SortComparator)clazz.newInstance();
                
            } catch ( Exception e ) {
                throw new RuntimeException( e );
            }
            
        }

        @Override
        public void map( StructReader key,
                         StructReader value ) {

            key   = StructReaders.wrap( key.toByteArray() );
            value = StructReaders.wrap( value.toByteArray() );

            StructReader sortKey = comparator.getSortKey( key , value );
            
            addSample( sortKey );

        }

        /**
         * Add a sample to the 
         */
        public void addSample( StructReader sr ) {

            // we are done sampling.
            if ( sample.size() >= MAX_SAMPLE_SIZE )
                return;

            sample.add( sr );

        }
        
        @Override
        public void close() throws IOException {

            //at this point we should have all the samples, sort then and then
            //determine partition boundaries.

            List<StructReader> boundaries = computePartitionBoundaries( config, getPartition(), comparator, sample );
            
            long partition_id = 0;

            for( StructReader boundary : boundaries ) {
                
                StructReader key = StructReaders.wrap( partition_id );

                log.info( "FIXME: Using boundary %s for partition: %s" , boundary.toInteger(), partition_id );
                
                emit( key, boundary );
                ++partition_id;
            }
            
        }

        @Override
        public void emit( StructReader key, StructReader value ) {

            log.info( "Going to emit partition table entry: %s=%s (%s)" , key.slice().readLong(), Hex.encode( value ), value.toInteger() );
            super.emit( key, value );
            
        }
        
    }

    public static class Reduce extends Reducer {

        private static final Logger log = Logger.getLogger();

        private List<StructReader> boundaries = new ArrayList();

        @Override
        public void reduce( StructReader key, List<StructReader> values ) {

            StructReader value = mean( values );

            //log.info( "FIXME: using values: %s with result %s on partition %s", format( values ), value.toInteger(), key.slice().readLong() );

            log.info( "%s", format( String.format( "FIXME: mean result=%s %s", format( value ), partition ), values ) );

            log.info( "Going to use final broadcast partition boundary: %s", Hex.encode( value ) );
            boundaries.add( value );

            if ( boundaries.size() == getConfig().getMembership().size() ) {
                emit( key, StructReaders.wrap( boundaries ) );
            }
            
        }

        private String format( StructReader sr ) {
            return String.format( "%s(%s)", sr.toInteger(), sr.toString() );
        }

        private String format( String desc, List<StructReader> list ) {

            StringBuilder buff = new StringBuilder();

            buff.append( "\n" );
            
            for( StructReader sr : list ) {
                buff.append( String.format( "%s: %s\n", desc, format( sr ) ) );
            }

            return buff.toString();
            
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
