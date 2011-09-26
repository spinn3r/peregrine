package maprunner.test;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import java.security.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.shuffle.*;

public class TestSorter {

    public void test1() throws Exception {

        System.out.printf( "------------ test1\n" );
        
        //test running with two lists which each have different values.

        byte[] d =  new byte[2];
        
        ChunkReader left  = makeTestSortChunk( new int[] { 0, 1, 2 } );
        ChunkReader right = makeTestSortChunk( new int[] { 3, 4, 5 } );

        List<ChunkReader> work = new ArrayList();
        work.add( left );
        work.add( right );

        _test( work );

    }

    public void test2() throws Exception {

        System.out.printf( "------------ test2\n" );
        
        //test running with two lists which each have different values.

        byte[] d =  new byte[2];
        
        ChunkReader left  = makeTestSortChunk( new int[] { 0, 1, 2 } );
        ChunkReader right = makeTestSortChunk( new int[] { 0, 1, 2 } );

        List<ChunkReader> work = new ArrayList();
        work.add( left );
        work.add( right );

        _test( work );

    }

    private void _test( List<ChunkReader> work ) throws Exception {

        new Sorter( new SortListener() {

                public void onFinalValue( byte[] key, List<byte[]> values ) {

                    /*
                    ByteArrayListValue lv = new ByteArrayListValue();
                    lv.fromBytes( values );
                    
                    List<byte[]> list = lv.getValues();
                    */
                    
                    System.out.printf( "sorted value: key=%s, value=%s\n", Hex.encode( key ), values );

                }

            } ).sort( work );

    }
    
    public static void main( String[] args ) throws Exception {

        TestSorter t = new TestSorter();

        t.test1();
        t.test2();
        
    }

    public static Tuple[] makeTestTupleArray( int[] input ) {

        Tuple[] result = new Tuple[ input.length ];

        for( int i = 0; i < input.length; ++i ) {

            result[i] = new Tuple( new IntKey( input[i] ), new IntValue( input[i]  ) );
            
        }

        return result;
        
    }

    public static ChunkReader makeTestSortChunk( int[] input ) throws IOException {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ChunkWriter writer = new ChunkWriter( out );

        for( int i = 0; i < input.length; ++i ) {

            long val = input[i];
            
            byte[] hash = LongBytes.toByteArray( (long) val );
            
            System.out.printf( "%d encodes as: %s\n", val, Hex.encode( hash ) );
            
            ByteArrayKey key = new ByteArrayKey( hash );

            writer.write( key.toBytes(), new IntValue( input[i] ).toBytes() );
            
        }

        writer.close();

        return new ChunkReader( out.toByteArray() );
        
    }

}
