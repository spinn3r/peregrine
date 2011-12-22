package peregrine.reduce;

import java.util.*;

import peregrine.controller.*;
import peregrine.io.*;
import peregrine.keys.*;
import peregrine.util.primitive.IntBytes;
import peregrine.io.partition.*;
import peregrine.task.*;

/**
 * Tests running a reduce but also has some code to benchmark them so that we
 * can look at the total performance.
 */
public class TestLocalReducerPerformance extends peregrine.BaseTestWithMultipleConfigs {

    public void test1() throws Exception {

        String path = String.format( "/test/%s/test1.in", getClass().getName() );
        
        ExtractWriter writer = new ExtractWriter( config, path );

        int max = 10000;
        
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
        System.setProperty( "peregrine.test.config", "1:1:1" ); 
        runTests();
    }

}