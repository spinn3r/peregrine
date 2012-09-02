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
            
            for( long i = 0; i < nr_partitions - 1; ++i ) {

                offset += width;
                
                StructReader val = sample.get( offset - 1 );

                log.info( "Using partition boundary: %s", Hex.encode( val ) );
                partitionTable.emit( StructReaders.wrap( i ), val );
            }
            
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