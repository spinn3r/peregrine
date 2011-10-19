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

        int max = 2;
        
        ChunkReader reader = _test( makeRandomSortChunk( max ) );

        Tuple last = null;

        FullTupleComparator comparator = new FullTupleComparator();

        int count = 0;
        while( reader.hasNext() ) {

            byte[] key = reader.key();
            byte[] value = reader.value();

            System.out.printf( "%s\n", Hex.encode( key ) );

            Tuple t = new Tuple( key, value );

            if ( last != null && comparator.compare( last, t ) > 0 )
                throw new RuntimeException();

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

            long val = input[i];
            
            byte[] hash = Hashcode.getHashcode( "" + val );
            
            //System.out.printf( "%d encodes as: %s\n", val, Hex.encode( hash ) );
            
            ByteArrayKey key = new ByteArrayKey( hash );

            writer.write( key.toBytes(), new IntValue( input[i] ).toBytes() );
            
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
