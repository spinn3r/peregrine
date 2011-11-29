package peregrine.shuffle;

import peregrine.*;
import peregrine.util.*;
import peregrine.util.primitive.LongBytes;
import peregrine.values.*;
import peregrine.config.Partition;
import peregrine.controller.*;
import peregrine.io.*;

/**
 * Test the FULL shuffle path, not just pats of it...including running with two
 * daemons, writing a LOT of data, and then reading it back in correctly as if
 * we were a reducer.
 */
public class TestShuffleInputChunkReader extends peregrine.BaseTestWithMultipleDaemons {

    public static class Map extends Mapper {

        @Override
        public void map( StructReader key,
                         StructReader value ) {

            emit( key, value );
            
        }

    }

    public TestShuffleInputChunkReader() {
        super( 2, 2, 5 );
    }
    
    public void test1() throws Exception {

        String path = "/tmp/test.in";
        
        ExtractWriter writer = new ExtractWriter( config, path );

        int max = 10000;
        
        for( long i = 0; i < max; ++i ) {

        	StructReader key =StructReaders.create( i );
        	StructReader value = key;
            writer.write( key, value );
            
        }

        writer.close();
        
        Controller controller = new Controller( config );

        try {
            controller.map( Map.class, path );
        } finally {
            controller.shutdown();
        }

        //now create a ParallelShuffleInputChunkReader for one of the
        //partitions so that we can see if it actually works

        path = "/tmp/peregrine-fs/localhost/11116/tmp/shuffle/default/0000000000.tmp";

        Partition partition = new Partition( 4 );

        ShuffleInputChunkReader reader = new ShuffleInputChunkReader( configs.get(4), partition, path );

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
