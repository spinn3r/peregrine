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
public class TestFullShufflePath extends peregrine.BaseTestWithTwoDaemons {

    private void doTestIter( ShuffleJobOutput output, int max_emits ) throws Exception {

        for ( int i = 0; i < max_emits; ++i ) {

            byte[] key = LongBytes.toByteArray( i );
                ;

            byte[] value = new byte[] { (byte)'x', (byte)'x', (byte)'x', (byte)'x', (byte)'x', (byte)'x', (byte)'x' };

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

        controller.shutdown();

        // now the data should be on disk... try to read it back out wth a ShuffleInputChunkReader

        int count = 0;
        
        count += readShuffle( "/tmp/peregrine-fs/localhost/11112/tmp/shuffle/default/0000000000.tmp", 0 );
        count += readShuffle( "/tmp/peregrine-fs/localhost/11113/tmp/shuffle/default/0000000000.tmp", 1 );

        assertEquals( count, max_emits );

        ShuffleInputChunkReader reader = new ShuffleInputChunkReader( "/tmp/peregrine-fs/localhost/11112/tmp/shuffle/default/0000000000.tmp", 0 );

        while( reader.hasNext() ) {

            System.out.printf( "key: %s, value: %s\n", Hex.encode( reader.key() ), Hex.encode( reader.value() ) );
            
        }

        //ChunkSorter sorter = new ChunkSorter( config , new Partition( 0 ), new ShuffleInputReference( "default" ) );

        //ChunkReader result = sorter.sort( reader );

        //eregrine.reduce.sorter.TestChunkSorter.assertResults( result, max_emits );

    }

    private int readShuffle( String path, int partition ) throws IOException {

        ShuffleInputChunkReader reader = new ShuffleInputChunkReader( path, partition );

        assertTrue( reader.size() > 0 );

        int count = 0;
        while( reader.hasNext() ) {

            System.out.printf( "key: %s, value: %s\n", Hex.encode( reader.key() ), Hex.encode( reader.value() ) );

            ++count;

        }

        assertEquals( reader.size(), count );

        System.out.printf( "Read count: %,d\n", count );

        return count;
        
    }
    
    public void test1() throws Exception {

        for( int i = 5; i < 10; ++i ) {
            doTest( 2, i );
        }
        
        //doTest( 3, 100 );

        //doTest( 2, 5000000 );

        //doTest( 10, 3 );
        //doTest( 3, 100 );
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
