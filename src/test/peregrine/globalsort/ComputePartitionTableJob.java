package peregrine.globalsort;

import java.util.*;
import java.io.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.reduce.*;

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
            
        }

    }

    public static class Reduce extends Reducer {

        @Override
        public void reduce( StructReader key, List<StructReader> values ) {

        }
        
    }

}