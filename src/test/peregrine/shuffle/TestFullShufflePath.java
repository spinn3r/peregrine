package peregrine.shuffle;

import java.io.*;
import peregrine.*;
import peregrine.util.*;
import peregrine.util.primitive.LongBytes;
import peregrine.reduce.*;
import peregrine.config.Partition;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.shuffle.sender.*;
import peregrine.reduce.sorter.*;

/**
 * Test the FULL shuffle path, not just pats of it...including running with two
 * daemons, writing a LOT of data, and then reading it back in correctly as if
 * we were a reducer.
 */
public class TestFullShufflePath extends peregrine.BaseTestWithMultipleDaemons {

    public TestFullShufflePath() {
        super( 1, 1, 2 );
    }
    
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

        ShuffleJobOutput output = new ShuffleJobOutput( config, new Partition( 0 ) );

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

        ShuffleInputChunkReader reader = new ShuffleInputChunkReader( configs.get( 0 ), new Partition( 0 ), "/tmp/peregrine-fs/localhost/11112/tmp/shuffle/default/0000000000.tmp" );

        while( reader.hasNext() ) {
            reader.next();
            //System.out.printf( "key: %s, value: %s\n", Hex.encode( reader.key() ), Hex.encode( reader.value() ) );
        }

        ChunkSorter sorter = new ChunkSorter( configs.get( 0 ) , new Partition( 0 ), new ShuffleInputReference( "default" ) );

        File file_input = new File( "/tmp/peregrine-fs/localhost/11112/tmp/shuffle/default/0000000000.tmp" );
        File file_output = new File( "/tmp/test.out" );

        System.out.printf( "===============================\n" );
        
        ChunkReader result = sorter.sort( file_input, file_output );

        assertResults( result, max_emits );

    }

    public static void assertResults( ChunkReader reader, int max ) throws Exception {

        byte[] last = null;

        FullKeyComparator comparator = new FullKeyComparator();

        int count = 0;
        while( reader.hasNext() ) {
            
            byte[] key = reader.key();
            byte[] value = reader.value();

            System.out.printf( "%s\n", Hex.encode( key ) );
            
            if ( last != null && comparator.compare( last, value ) > 0 )
                throw new RuntimeException( "value is NOT less than last value" );

            LongBytes.toByteArray( count );

            /*
            if ( last != null && comparator.compare( last, new Tuple( correct, correct ) ) == 0 ) {

                String message = "value is NOT correct";
                
                throw new RuntimeException( message );
            }
            */
            
            last = value;
            ++count;
            
        }

        assertTrue( count > 0 );
        assertEquals( reader.size(), count );
        
        //assertEquals( max, count );

    }

    private int readShuffle( String path, int partition ) throws IOException {

        ShuffleInputChunkReader reader = new ShuffleInputChunkReader( configs.get( partition ), new Partition( partition ), path );

        assertTrue( reader.size() > 0 );

        int count = 0;
        while( reader.hasNext() ) {

            reader.next();
            //System.out.printf( "readShuffle: partition: %s, key: %s, value: %s\n",
            //                   partition, Hex.encode( reader.key() ), Hex.encode( reader.value() ) );

            ++count;

        }

        assertEquals( reader.size(), count );

        System.out.printf( "Read count: %,d\n", count );

        return count;
        
    }
    
    public void test1() throws Exception {

        // FIXME sort BOTH partitions and make sure the counts are right.
        // 
        
        //doTest( 2, 0 );  FIXME: this doesn't work.
        //doTest( 2, 1 );
        //doTest( 2, 2 );
        // doTest( 2, 3 );
        // doTest( 2, 10 );
        // doTest( 2, 100 );
        // doTest( 2, 1000 );
        // doTest( 2, 10000 );
        doTest( 2, 100000 );
        // doTest( 2, 10000000 );

        //doTest( 3, 100 );

        //doTest( 2, 5000000 );

        //doTest( 10, 3 );
        //doTest( 3, 100 );

    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
