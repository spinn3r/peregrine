package peregrine.reduce.sorter;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import java.security.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.reduce.FullTupleComparator;

public class TestChunkSorter extends peregrine.BaseTestWithTwoDaemons {

    /**
     * test running with two lists which each have different values.
     */
    public void test1() throws Exception {

        // this actually does break us.. interesting.
        //int max = 500000;

        int max = 4;
        
        assertResults( _test( makeRandomSortChunk( max ) ), max );
        
    }

    /*
    public void test2() throws Exception {

        for( int i = 0; i < 1024; ++i ) {
            assertResults( _test( makeRandomSortChunk( i ) ), i );
        }
        
    }

    */
    public static void assertResults( ChunkReader reader, int max ) throws Exception {

        Tuple last = null;

        FullTupleComparator comparator = new FullTupleComparator();

        int count = 0;
        while( reader.hasNext() ) {

            byte[] key = reader.key();
            byte[] value = reader.value();

            System.out.printf( "%s\n", Hex.encode( key ) );
            
            Tuple t = new Tuple( key, value );

            if ( last != null && comparator.compare( last, t ) > 0 )
                throw new RuntimeException( "value is NOT less than last value" );

            // now make sure it's the RIGHT value.
            byte[] correct = LongBytes.toByteArray( count );

            if ( last != null && comparator.compare( last, new Tuple( correct, correct ) ) == 0 ) {

                String message = "value is NOT correct";
                
                throw new RuntimeException( message );
            }
            
            last = t;
            ++count;
            
        }

        assertEquals( max, count );

    }
    
    private ChunkReader _test( ChunkReader reader ) throws Exception {

        ChunkSorter2 sorter = new ChunkSorter2( config , new Partition( 0 ), new ShuffleInputReference( "default" ) );

        ChunkReader result = sorter.sort( reader );

        return result;
        
    }

    public static ChunkReader makeRandomSortChunk( int nr_values ) throws IOException {

        int[] values = new int[nr_values];

        Random r = new Random();
        
        for( int i = 0; i < nr_values; ++i ) {
            values[i] = r.nextInt();
        }

        return makeTestSortChunk( values );
        
    }

    public static ChunkReader makeTestSortChunk( int[] input ) throws IOException {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ChunkWriter writer = new DefaultChunkWriter( out );

        for( int i = 0; i < input.length; ++i ) {

            byte[] key = LongBytes.toByteArray( i );

            // set the value to 'x' so that I don't accidentally read the key.
            byte[] value = new byte[] { (byte)'x', (byte)'x', (byte)'x', (byte)'x', (byte)'x', (byte)'x', (byte)'x' };
            
            writer.write( key, value );
            
        }

        writer.close();

        byte[] data = out.toByteArray();

        System.out.printf( "Working with chunk of %,d bytes\n", data.length );
        
        return new DefaultChunkReader( data );
        
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
