package peregrine.shuffle;

import java.io.*;
import peregrine.*;
import peregrine.util.*;
import peregrine.util.primitive.LongBytes;
import peregrine.values.*;
import peregrine.reduce.*;
import peregrine.config.Partition;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.shuffle.sender.*;
import peregrine.reduce.sorter.*;

/**
 * Test the FULL shuffle path, not just pats of it...including running with two
 * daemons, writing a LOT of data, and then reading it back in correctly as if
 * we were a reducer.
 */
public class TestShufflePerformance extends BaseTestWithMultipleConfigs {

    public void doTest() throws Exception {

        byte[] value = new byte[32];

        int write_width = 2 * 8 * value.length;
        
        int output_size = 100000 * getFactor();
        
        int max_emits = (output_size / write_width) / config.getReplicas();
        
        System.out.printf( "Running with %,d hosts.\n", config.getHosts().size() );

        Controller controller = new Controller( config );

        try {

            long before = System.currentTimeMillis();
            
            ShuffleJobOutput output = new ShuffleJobOutput( config, new Partition( 0 ) );

            ChunkReference chunkRef = new ChunkReference( new Partition( 0 ) );
            chunkRef.local = 0;

            // we need to call onChunk to init the shuffle job output.
            output.onChunk( chunkRef );

            for ( long i = 0; i < max_emits; ++i ) {

                StructReader key = StructReaders.wrap( i );
                
                output.emit( key, key );
                
            }

            output.onChunkEnd( chunkRef );

            output.close();

            long after = System.currentTimeMillis();

            long duration = (after-before);

            long throughput = -1;

            try {
                throughput = output.length() / ( duration / 1000 );
            } catch ( Throwable t ) {}

            System.out.printf( "Wrote %,d bytes with duration %,d ms at %,d b/s\n", output.length(), duration, throughput );
            
            controller.flushAllShufflers();

        } finally {
            controller.shutdown();
        }

    }

    public static void main( String[] args ) throws Exception {

        //System.setProperty( "peregrine.test.factor", "35" ); 
        System.setProperty( "peregrine.test.factor", "20000" ); 
        System.setProperty( "peregrine.test.config", "1:1:256" ); 
        //System.setProperty( "peregrine.test.config", "1:1:1" ); 

        runTests();
    }

}
