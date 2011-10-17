package peregrine.io;

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

public class TestNewShuffleJobOutput extends peregrine.BaseTestWithTwoDaemons {

    private void doTestIter( int max_emits ) throws Exception {

        ShuffleJobOutput output = new ShuffleJobOutput( config );

        ChunkReference chunkRef = new ChunkReference( new Partition( 0  ) );
        chunkRef.local = 0;

        output.onChunk( chunkRef );

        for ( int i = 0; i < max_emits; ++i ) {

            byte[] key = new StructWriter()
                .writeHashcode( "" + i )
                .toBytes()
                ;

            byte[] value = key;

            output.emit( key, value );
            
        }

        output.onChunkEnd( chunkRef );

        output.close();

        Controller controller = new Controller( config );

        controller.flushAllShufflers();

        controller.shutdown();
        
    }

    public void doTest( int iterations, int max_emits ) throws Exception {

        assertEquals( config.getHosts().size(), 2 );

        System.out.printf( "Running with %,d hosts.\n", config.getHosts().size() );
        
        for( int i = 0; i < iterations; ++i ) {
            doTestIter( max_emits );
        }

    }
    
    public void test1() throws Exception {
        doTest( 20, 1 );
        doTest( 100, 3 );
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
