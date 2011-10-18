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
import peregrine.io.chunk.*;

public class TestChunkSorter extends peregrine.BaseTestWithTwoDaemons {

    /**
     * test running with two lists which each have different values.
     */
    public void test1() throws Exception {

        ChunkReader reader = _test( makeRandomSortChunk( 500 ) );

        Tuple last = null;

        FullTupleComparator comparator = new FullTupleComparator();
        
        while( reader.hasNext() ) {

            byte[] key = reader.key();
            byte[] value = reader.value();

            Tuple t = new Tuple( key, value );
            
            System.out.printf( "READ: %s\n", Hex.encode( key ) );
            
            if ( last != null && comparator.compare( last, t ) > 0 )
                throw new RuntimeException();

            last = t;
            
        }
        
    }

    public void test2() throws Exception {

        System.out.printf( "test2\n" );
        
        int max = 10;

        System.out.printf( "writing to array: \n" );

        Tuple[] array = new Tuple[max];

        for( int i = 0; i < 10; ++i ) {
            array[i] = new Tuple( new byte[0], new byte[0] );
        }

        System.out.printf( "reading from it.\n" );
        
        TupleArrayChunkReader reader = new TupleArrayChunkReader( array ) ;

        int count = 0;
        while( reader.hasNext() ) {

            byte[] key = reader.key();
            byte[] value = reader.value();

            ++count;

            System.out.printf( "." );
            
        }

        System.out.printf( "\n" );
        
        assertEquals( count, max );
        
    }
    
    private ChunkReader _test( ChunkReader reader ) throws Exception {

        ChunkSorter sorter = new ChunkSorter( config , new Partition( 0 ), new ShuffleInputReference( "default" ) );

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
            
            System.out.printf( "%d encodes as: %s\n", val, Hex.encode( hash ) );
            
            ByteArrayKey key = new ByteArrayKey( hash );

            writer.write( key.toBytes(), new IntValue( input[i] ).toBytes() );
            
        }

        writer.close();

        return new DefaultChunkReader( out.toByteArray() );
        
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
