package peregrine.globalsort;

import java.util.*;
import java.io.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.reduce.*;
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

    public static final byte[] FIRST_BOUNDARY = new byte[ KEY_LEN ]; 
    public static final byte[] LAST_BOUNDARY  = new byte[ KEY_LEN ]; 

    static {

        // the last boundary should be all ones.
        for( int i = 0; i < KEY_LEN; ++i ) {
            LAST_BOUNDARY[i] = 1;
        }
        
    }
    
    public static class Map extends Mapper {

        /**
         * The sample data 
         */
        List<StructReader> sample = new ArrayList();

        private JobOutput partitionTable = null;

        @Override
        public void init( List<JobOutput> output ) {

            super.init( output );
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

            Collections.sort( sample, new StrictStructReaderComparator() );

            //now write out the partitions.

            int nr_partitions = getConfig().getMembership().size();

            int width = sample.size() / nr_partitions;

            int offset = 0;

            log.info( "Going to split across %,d partitions" , nr_partitions );

            StructReader lastEmittedBoundary = StructReaders.wrap( FIRST_BOUNDARY );

            int partition_id = 0;
            
            for( ; partition_id < nr_partitions - 1; ++partition_id ) {

                offset += width;
                
                StructReader currentBoundary = sample.get( offset - 1 );

                log.info( "Using partition boundary: %s", Hex.encode( currentBoundary ) );
                
                partitionTable.emit( StructReaders.wrap( partition_id ), StructReaders.join( lastEmittedBoundary,
                                                                                             currentBoundary ) );

                lastEmittedBoundary = currentBoundary;
                
            }

            partitionTable.emit( lastEmittedBoundary, StructReaders.join( lastEmittedBoundary, 
                                                                          StructReaders.wrap( LAST_BOUNDARY ) ) );
                
        }

    }

    public static class Reduce extends Reducer {

        private static final Logger log = Logger.getLogger();

        @Override
        public void reduce( StructReader key, List<StructReader> values ) {

            if ( values.size() > 0 ) {
            
                int len = values.get( 0 ).length();

                int sum = 0;

                byte[] result = new byte[len];

                byte[] last_key = new byte[ KEY_LEN ];
                
                for( int i = 0; i < len; ++i ) {

                    for( StructReader current : values ) {
                        sum += current.getByte( i ) & 0xFF;
                    }

                    result[i] = (byte)(sum / values.size());
                    
                }

                StructReader value = StructReaders.wrap( result );

                log.info( "Going to emit midpoint: %s", Hex.encode( value ) );
                
                emit( key, value );
                
            }
                
        }
        
    }

}