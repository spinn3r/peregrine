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

    public static final StructReader FIRST_BOUNDARY = createByteArray( 0, KEY_LEN ); 
    public static final StructReader LAST_BOUNDARY  = createByteArray( 1, KEY_LEN );

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

            StructReader lastEmittedBoundary = FIRST_BOUNDARY;

            long partition_id = 0;

            for( ; partition_id < nr_partitions - 1; ++partition_id ) {

                offset += width;
                
                StructReader currentBoundary = sample.get( offset - 1 );

                log.info( "Using partition boundary: %s", Hex.encode( currentBoundary ) );

                StructReader key = StructReaders.wrap( partition_id );

                partitionTable.emit( key, StructReaders.join( lastEmittedBoundary, currentBoundary ) );

                lastEmittedBoundary = currentBoundary;
                
            }

            StructReader key = StructReaders.wrap( partition_id );
            
            partitionTable.emit( key, StructReaders.join( lastEmittedBoundary, LAST_BOUNDARY ) );
                
        }

    }

    public static class Reduce extends Reducer {

        private static final Logger log = Logger.getLogger();

        private int partition_id = 0;

        private int nr_partitions = -1;

        private StructReader last = null;
        
        @Override
        public void init( List<JobOutput> output ) {

            super.init( output );

            nr_partitions = getConfig().getMembership().size();

        }

        @Override
        public void reduce( StructReader key, List<StructReader> values ) {

            if ( partition_id == 0 ) {

                //first partition

                last = mean( ending( parse( values ) ) );
                emit( key, last );
                
            } else if ( partition_id == nr_partitions - 1 ) {
                //last partition
                emit( key, LAST_BOUNDARY );
            } else {
                last = mean( starting( parse( values ) ) );
                emit( key, last );
            }
            
            ++partition_id;

        }

        private StructReader mean( List<StructReader> values ) {

            int sum = 0;

            byte[] result = new byte[ KEY_LEN ];

            byte[] last_key = new byte[ KEY_LEN ];
            
            for( int i = 0; i < KEY_LEN; ++i ) {

                for( StructReader current : values ) {
                    sum += current.getByte( i ) & 0xFF;
                }

                result[i] = (byte)(sum / values.size());
                sum = 0;
                
            }

            return StructReaders.wrap( result );

        }

        /**
         * Take a list of struct readers and build then into a list of
         * PartitionRange objects representing the start and end of a partition
         * range.
         */         
        private List<PartitionRange> parse( List<StructReader> list ) {

            List<PartitionRange> result = new ArrayList();
            
            for( StructReader current : list ) {

                log.info( "FIXME: length : %s", current.length() );
                
                result.add( new PartitionRange( current.readSlice( KEY_LEN ),
                                                current.readSlice( KEY_LEN ) ) );
                
            }

            return result;
            
        }

        private List<StructReader> starting( List<PartitionRange> list ) {

            List<StructReader> result = new ArrayList();

            for( PartitionRange current : list ) {
                result.add( current.start );
            }

            return result;
            
        }

        private List<StructReader> ending( List<PartitionRange> list ) {

            List<StructReader> result = new ArrayList();

            for( PartitionRange current : list ) {
                result.add( current.end );
            }

            return result;
            
        }

        /**
         * Represents the start and end of a partition range.
         */
        class PartitionRange {

            public StructReader start;
            public StructReader end;

            public PartitionRange( StructReader start, StructReader end ) {
                this.start = start;
                this.end = end;
            }

        }
        
    }

}