package peregrine.shuffle;

import peregrine.*;
import peregrine.util.*;
import peregrine.util.primitive.LongBytes;
import peregrine.config.Partition;
import peregrine.io.*;

/**
 * Test the FULL shuffle path, not just pats of it...including running with two
 * daemons, writing a LOT of data, and then reading it back in correctly as if
 * we were a reducer.
 */
public class TestParallelShuffleInputChunkReader extends peregrine.BaseTestWithMultipleDaemons {

    public static class Map extends Mapper {

        @Override
        public void map( byte[] key,
                         byte[] value ) {

            emit( key, value );
            
        }

    }

    public TestParallelShuffleInputChunkReader() {
        super( 2, 2, 5 );
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

        //now create a ParallelShuffleInputChunkReader for one of the
        //partitions so that we can see if it actually works

        path = "/tmp/peregrine-fs/localhost/11116/tmp/shuffle/default/0000000000.tmp";

        ShuffleInputChunkReader reader = new ShuffleInputChunkReader( configs.get(4), new Partition( 4 ), path );

        assertTrue( reader.size() > 0 );

        while( reader.hasNext() ) {

            reader.next();
            
            System.out.printf( "%s = %s\n", Hex.encode( reader.key() ), Hex.encode( reader.value() ) );
            
        }
        
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
