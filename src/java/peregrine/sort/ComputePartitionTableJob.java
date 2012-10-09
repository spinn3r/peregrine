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
     * The maximum number of keys to read into memory to compute the training
     * set..
     * 
     * <p> TODO: Technically this should be a function of the number of
     * partitions because our accuracy will fail with the total number of
     * partitions we have.
     */
    public static final int MAX_TRAIN_SIZE = 100000; 

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
                                                                 List<StructReader> train ) {

        log.info( "Training set size is: %,d items", train.size() );

        List<StructReader> result = new ArrayList();

        if ( train.size() == 0 ) {

            // It is totally reasonable to want to sort a file with no
            // entries.
            
            log.warn( "No training data for job." );

            return result;
            
        }

        Collections.sort( train, comparator );

        // now down sample the training set so that we can grok what is
        // happening by looking at a smaller number of items.  In practice the
        // partition boundaries won't be off significantly using this technique.

        for( StructReader current : summarize( train, 80 ) ) {
            log.info( "Training dataset summary on %s %s", part, format( current ) );
        }
        
        int nr_partitions = config.getMembership().size();

        result = summarize( train, nr_partitions );

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
         * The training data.  We sort this list and then take trains at
         * partition boundaries.
         */
        private List<StructReader> train = new ArrayList();

        /**
         * The testing data for verifying the training data.
         */
        private List<StructReader> test = new ArrayList();

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
            // full range of key data since we only read one chunk to train.
            
            key   = StructReaders.wrap( new byte[8] );
            value = StructReaders.wrap( value.toByteArray() );

            // first try to enter it as a training point.
            if ( addTrain( key, value ) == false ) {

                // if that fails we should keep it as a testing point.
                addTest( key, value );
                
            }

        }

        /**
         * Add a data point to the training set. 
         */
        protected boolean addTrain( StructReader key, StructReader value ) {

            // we are done sampling.
            if ( train.size() >= MAX_TRAIN_SIZE )
                return false;

            train.add( comparator.getSortKey( key , value ) );

            return true;
            
        }

        /**
         * Add a data point to the testing set. 
         */
        protected boolean addTest( StructReader key, StructReader value ) {

            // we are done sampling.
            if ( test.size() >= MAX_TRAIN_SIZE )
                return false;

            test.add( comparator.getSortKey( key , value ) );

            return true;
            
        }

        /**
         * From the given input, create synthetic data so that we `target`
         * results.
         */
        private List<StructReader> createSyntheticTrainData( Collection<StructReader> input, int target ) {

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

            Collections.sort( train, comparator );

            if ( train.size() == 0  ) {
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

            for ( StructReader sr : train ) {
                set.add( sr );
            }

            // If we have too few training data points we have to create
            // synthetic data and then generate new training data and then use
            // the remaining eight(8) bytes to distribute values among the
            // cluster.  The problem is that since we only read one chunk we're
            // not getting the full key space but we can just create synthetic
            // keys anyway.  The advantage to this approach is that we can
            // re-use a lot of existing code.

            // ALWAYS making synthetic partitions when we have less than
            // MAX_TRAIN_SIZE ... this train size should be our target so that
            // we have fined grained partitioning of the data.
            if ( set.size() < MAX_TRAIN_SIZE ) {

                log.info( "Creating synthetic training data due to small training set size: %s", set.size() );
                
                train = createSyntheticTrainData( set, MAX_TRAIN_SIZE );
                Collections.sort( train, comparator );

            }

            log.info( "Working with %,d training set entries. " , train.size() );

            //at this point we should have all the trains, sort then and then
            //determine partition boundaries.

            List<StructReader> boundaries = computePartitionBoundaries( config, getPartition(), comparator, train );

            test( boundaries );
            
            long partition_id = 0;

            for( StructReader boundary : boundaries ) {
                StructReader key = StructReaders.wrap( partition_id );
                emit( key, boundary );
                ++partition_id;
            }
            
        }

        public void test( List<StructReader> boundaries ) {

            GlobalSortPartitioner partitioner = new GlobalSortPartitioner();
            partitioner.init( job, boundaries );
            partitioner.setComparator( comparator );

            int nr_partitions = config.getMembership().size();

            java.util.Map<Partition, Integer> hits = new TreeMap();
            
            for( int i = 0; i < nr_partitions; ++i ) {
                hits.put( new Partition( i ), 0 );
            }

            for( StructReader b : boundaries ) {

                // this is a composite SortKey so we have to read the key as the
                // last 8 bytes and the value as the first 8 bytes.

                StructReader key = b.slice( 8, 8 );
                StructReader value = b.slice( 0, 8 );
                
                Partition part = partitioner.partition( key, value );

                hits.put( part, hits.get( part ) + 1 );
                
            }

            //TODO: analyze the hits and throw an Exception if they don't look
            //good.
            
            log.info( "Test results: %s", hits );
            
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
