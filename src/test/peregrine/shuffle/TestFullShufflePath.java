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

/**
 * Test the FULL shuffle path, not just pats of it...including running with two
 * daemons, writing a LOT of data, and then reading it back in correctly as if
 * we were a reducer.
 */
public class TestFullShufflePath extends peregrine.BaseTestWithTwoDaemons {

    private void doTestIter( ShuffleJobOutput output, int max_emits ) throws Exception {

        for ( int i = 0; i < max_emits; ++i ) {

            byte[] key = new StructWriter()
                .writeHashcode( "" + i )
                .toBytes()
                ;

            byte[] value = key;

            output.emit( key, value );
        }

    }

    public void doTest( int iterations, int max_emits ) throws Exception {

        assertEquals( config.getHosts().size(), 2 );

        System.out.printf( "Running with %,d hosts.\n", config.getHosts().size() );

        Controller controller = new Controller( config );

        ShuffleJobOutput output = new ShuffleJobOutput( config );

        for( int i = 0; i < iterations; ++i ) {

            ChunkReference chunkRef = new ChunkReference( new Partition( 0  ) );
            chunkRef.local = i;

            // we need to call onChunk to init the shuffle job output.
            output.onChunk( chunkRef );

            doTestIter( output, max_emits );

            output.onChunkEnd( chunkRef );

        }

        output.close();

        controller.flushAllShufflers();

        // now the data should be on disk... try to read it back out.

        controller.shutdown();

    }
    
    public void test1() throws Exception {
        doTest( 10, 3 );
        doTest( 3, 100 );
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
