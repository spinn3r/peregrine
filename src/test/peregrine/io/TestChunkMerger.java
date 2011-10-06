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
import peregrine.shuffle.*;
import peregrine.io.*;

public class TestChunkMerger extends junit.framework.TestCase {

    /**
     * test running with two lists which each have different values.
     */
    public void test1() throws Exception {

        System.out.printf( "------------ test1\n" );

        List<ChunkReader> work = new ArrayList();

        work.add( makeTestSortChunk( new int[] { 3, 4, 5 } ) );
        work.add( makeTestSortChunk( new int[] { 0, 1, 2 } ) );

        _test( work );

    }

    private void _test( List<ChunkReader> work ) throws Exception {

        new ChunkMerger( new SortListener() {
                
                public void onFinalValue( byte[] key, List<byte[]> values ) {
                    
                    List<String> pp = new ArrayList();

                    for( byte[] value : values ) {

                        IntValue v = new IntValue();
                        v.fromBytes( value );
                            
                        pp.add( v.toString() );
                        
                    }
                    
                    System.out.printf( "sorted value: key=%s, value=%s\n", Hex.encode( key ), pp );
                }

            } ).merge( work );

    }

    public static void main( String[] args ) throws Exception {

        TestChunkMerger t = new TestChunkMerger();
        t.test1();
        
    }

    public static ChunkReader makeTestSortChunk( int[] input ) throws IOException {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ChunkWriter writer = new DefaultChunkWriter( out );

        for( int i = 0; i < input.length; ++i ) {

            long val = input[i];
            
            byte[] hash = LongBytes.toByteArray( (long) val );
            
            System.out.printf( "%d encodes as: %s\n", val, Hex.encode( hash ) );
            
            ByteArrayKey key = new ByteArrayKey( hash );

            writer.write( key.toBytes(), new IntValue( input[i] ).toBytes() );
            
        }

        writer.close();

        return new DefaultChunkReader( out.toByteArray() );
        
    }

}
