package peregrine.reduce;

import java.util.*;

import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.keys.*;
import peregrine.task.*;
import peregrine.util.primitive.IntBytes;

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

        int max = 10000 * getFactor();
        
        for( int i = 0; i < max; ++i ) {
            
            byte[] key = new IntKey( i ).toBytes();
            byte[] value = new byte[1024];
            writer.write( key, value );
            
        }

        writer.close();
 
        Controller controller = new Controller( config );

        try {
            controller.map( peregrine.Mapper.class, path );
            controller.reduce( peregrine.Reducer.class, new Input(), new Output( "blackhole:" ) );
        } finally {
            controller.shutdown();
        }

    }

    public static void main( String[] args ) throws Exception {
        System.setProperty( "peregrine.test.factor", "30" ); 
        System.setProperty( "peregrine.test.config", "1:1:1" ); 
        runTests();
    }

}