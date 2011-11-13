package peregrine.io;

import peregrine.*;
import peregrine.values.*;
import peregrine.config.Partition;
import peregrine.io.chunk.*;
import peregrine.shuffle.sender.*;

public class TestNewShuffleJobOutput extends peregrine.BaseTestWithTwoDaemons {

    private void doTestIter( int max_emits ) throws Exception {

        Partition part = new Partition( 0 );
        
        ShuffleJobOutput output = new ShuffleJobOutput( config, part );

        ChunkReference chunkRef = new ChunkReference( part );
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
