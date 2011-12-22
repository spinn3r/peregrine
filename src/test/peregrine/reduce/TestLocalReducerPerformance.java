package peregrine.reduce;

import java.util.*;

import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.keys.*;
import peregrine.task.*;
import peregrine.util.primitive.*;

/**
 * Tests running a reduce but also has some code to benchmark them so that we
 * can look at the total performance.
 */
public class TestLocalReducerPerformance extends peregrine.BaseTestWithMultipleConfigs {

    @Override
    public void setUp() {

        super.setUp();
        
        for( Config config : configs ) {
            // so we can do post mortem on how much was written.
            config.setPurgeShuffleData( false );
        }
        
    }

    @Override
    public void doTest() throws Exception {

        String path = String.format( "/test/%s/test1.in", getClass().getName() );
        
        ExtractWriter writer = new ExtractWriter( config, path );

        int size = 10000000; // 10MB by default.
        int value_size = 1024;
        
        // each write 9 bytes per key, plus 2 bytes plus the the value length.
        int write_width = 9 + 2 + value_size;

        int writes = size / write_width;
        
        int max = writes * getFactor();
        
        for( long i = 0; i < max; ++i ) {
            
            byte[] key = LongBytes.toByteArray( i );
            byte[] value = new byte[value_size];
            writer.write( key, value );
            
        }

        writer.close();
 
        Controller controller = new Controller( config );

        try {

            controller.map( peregrine.Mapper.class, path );

            // drop caches here so that I can benchmark raw IO

            Linux.dropCaches();
            
            controller.reduce( peregrine.Reducer.class, new Input(), new Output( "blackhole:" ) );
            
        } finally {
            controller.shutdown();
        }

    }

    public static void main( String[] args ) throws Exception {

        System.setProperty( "peregrine.test.factor", "50" ); 
        System.setProperty( "peregrine.test.config", "1:1:1" ); 
        runTests();

    }

}