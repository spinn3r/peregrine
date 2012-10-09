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
import peregrine.util.primitive.*;

import com.spinn3r.log5j.*;

/**
 * Map reduce job which computes the partition routing table.
 */
public class ComputePartitionTableJob {

    private static final Logger log = Logger.getLogger();

    /**
     * The maximum number of keys to read into memory to compute the sample.
     * 
     * <p> TODO: Technically this should be a function of the number of
     * partitions because our accuracy will fail with the total number of
     * partitions we have.
     */
    public static final int MAX_SAMPLE_SIZE = 100000; 

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

        Collections.sort( sample, comparator );

        // now down sample the sample set so that we can grok what is happening
        // by looking at a smaller number of items.  In practice the partition
        // boundaries won't be off significantly using this technique.

        for( StructReader current : summarize( sample, 80 ) ) {
            log.info( "Sample dataset summary on %s %s", part, format( current ) );
        }
        
        int nr_partitions = config.getMembership().size();

        result = summarize( sample, nr_partitions );

        // we have to remove the last partition as it is pointless essentially.

        result.remove( result.size() - 1 );

        // add the max partition boundary.
        result.add( LAST_BOUNDARY );

        for( StructReader current : result ) {
            log.info( "Resulting partition table %s %s", part, format( current ) );
        }

        return result;

    }

    public static String format( StructReader sr ) {
        return String.format( "%22s(%s)", sr.toInteger(), sr.toString() );
    }
    
    /**
     * Summarize the given list by taking a reading every list.size() / count
     * items and return the result.  
     */
    public static List<StructReader> summarize( List<StructReader> list, int count ) {

        int width = list.size() / count;

        int offset = 0;

        List<StructReader> result = new ArrayList();
        
        for( int i = 0; i < count; ++i ) {

            offset += width;
            
            result.add( list.get( offset - 1) );
            
        }

        return result;
        
    }

    protected static SortComparator getSortComparator( Job job ) {

        try {

            Class clazz = job.getParameters().getClass( "sortComparator" );
            return (SortComparator)clazz.newInstance();

        } catch ( Exception e ) {
            throw new RuntimeException( e );
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
            comparator = getSortComparator( job );

        }

        @Override
        public void map( StructReader key,
                         StructReader value ) {

            // NOTE: we have to call toByteArray here because we are keeping
            // these values around for a long time and the maps might not be
            // valid when we try to read them later.

            // NOTE: by default only the value matters because we don't have a
            // full range of key data since we only read one chunk to sample.
            
            key   = StructReaders.wrap( new byte[8] );
            
            value = StructReaders.wrap( value.toByteArray() );

            addSample( key, value );

        }

        /**
         * Add a sample to the 
         */
        protected void addSample( StructReader key, StructReader value ) {

            // we are done sampling.
            if ( sample.size() >= MAX_SAMPLE_SIZE )
                return;

            sample.add( comparator.getSortKey( key , value ) );

        }

        /**
         * From the given input, create synthetic data so that we `target`
         * results.
         */
        private List<StructReader> createSyntheticSampleData( Collection<StructReader> input, int target ) {

            List<StructReader> result = new ArrayList();

            int per_input_target = target / input.size();
            int max_range = 65536;
            
            for ( StructReader sr : input ) {

                double width = max_range / (double)per_input_target;
                double offset = 0.0;
                
                for( int i = 0; i < per_input_target; ++i ) {

                    byte[] data = sr.toByteArray();
                    byte[] suffix = IntBytes.toByteArray( (int)offset );

                    data[8] = suffix[2];
                    data[9] = suffix[3];

                    offset += width;
                    
                    result.add( new StructReader( data ) );
                        
                }

            }

            return result;
            
        }

        @Override
        public void close() throws IOException {

            long nr_partitions = config.getMembership().size();

            Collections.sort( sample, comparator );

            if ( sample.size() == 0  ) {
                log.warn( "No values to sort." );

                // write all data to the last boundary position.  It doesn't
                // matter where we write these since there are no records being
                // written.
                for( long i = 0; i < nr_partitions; ++i ) {
                    StructReader key = StructReaders.wrap( i );
                    emit( key, LAST_BOUNDARY );
                }

                return;
            }

            //TODO: migrate this to HashSet for slightly better memory usage but
            //we have to have StructReader implement Comparable first.

            Set<StructReader> set = new TreeSet();

            for ( StructReader sr : sample ) {
                set.add( sr );
            }

            // If we have too few samples we have to create synthetic data and
            // then generate new samples and then use the remaining eight(8)
            // bytes to distribute values among the cluster.  The problem is
            // that since we only read one chunk we're not getting the full key
            // space but we can just create synthetic keys anyway.  The
            // advantage to this approach is that we can re-use a lot of
            // existing code.

            // TODO: consider ALWAYS making synthetic partitions when we have
            // less than MAX_SAMPLE_SIZE ... this sample size should be our
            // target so that we have fined grained partitioning of the data.
            if ( set.size() < (nr_partitions * 100) ) {

                log.info( "Creating synthetic sample data due to small sample set: %s", set.size() );
                
                sample = createSyntheticSampleData( set, MAX_SAMPLE_SIZE );
                Collections.sort( sample, comparator );

            }

            log.info( "Working with %,d samples. " , sample.size() );

            //at this point we should have all the samples, sort then and then
            //determine partition boundaries.

            List<StructReader> boundaries = computePartitionBoundaries( config, getPartition(), comparator, sample );
            
            long partition_id = 0;

            for( StructReader boundary : boundaries ) {
                
                StructReader key = StructReaders.wrap( partition_id );
                emit( key, boundary );
                ++partition_id;
            }
            
        }

        @Override
        public void emit( StructReader key, StructReader value ) {

            log.info( "Going to emit partition table entry: %s=%22s (%s)" , key.slice().readLong(), Hex.encode( value ), value.toInteger() );
            super.emit( key, value );
            
        }
        
    }

    public static class Reduce extends Reducer {

        private static final Logger log = Logger.getLogger();

        private List<StructReader> boundaries = new ArrayList();

        private SortComparator comparator = null;
        
        @Override
        public void init( Job job, List<JobOutput> output ) {

            super.init( job, output );
            comparator = getSortComparator( job );

        }

        @Override
        public void reduce( StructReader key, List<StructReader> values ) {

            StructReader value = median( values );
            
            log.info( "Going to use final broadcast partition boundary: %s", Hex.encode( value ) );
            boundaries.add( value );

            if ( boundaries.size() == getConfig().getMembership().size() ) {
                emit( key, StructReaders.wrap( boundaries ) );
            }
            
        }

        private StructReader median( List<StructReader> values ) {

            //TODO: technically the median DOES NOT change based on the
            //comparator so we don't really need this as a function of the job
            //and this just adds more code.
            
            Collections.sort( values, comparator );
            return values.get( values.size() / 2 );
            
        }

    }

}
