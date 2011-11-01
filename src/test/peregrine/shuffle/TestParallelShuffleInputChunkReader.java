package peregrine.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import java.security.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.reduce.*;
import peregrine.io.*;
import peregrine.io.async.*;
import peregrine.io.partition.*;
import peregrine.io.chunk.*;
import peregrine.pfsd.*;
import peregrine.shuffle.sender.*;
import peregrine.reduce.sorter.*;

/**
 * Test the FULL shuffle path, not just pats of it...including running with two
 * daemons, writing a LOT of data, and then reading it back in correctly as if
 * we were a reducer.
 */
public class TestParallelShuffleInputChunkReader extends peregrine.BaseTestWithTwoDaemons {

    public static class Map extends Mapper {

        @Override
        public void map( byte[] key,
                         byte[] value ) {

            emit( key, value );
            
        }

    }

    public void test1() throws Exception {

        String path = "/tmp/test.in";
        
        ExtractWriter writer = new ExtractWriter( config, path );

        int max = 10000;
        
        for( int i = 0; i < max; ++i ) {

            byte[] key = LongBytes.toByteArray( i );
            byte[] value = key;

            writer.write( key, value );
        }

        writer.close();
        
        Controller controller = new Controller( config );

        controller.map( Map.class, path );

        controller.shutdown();

    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
