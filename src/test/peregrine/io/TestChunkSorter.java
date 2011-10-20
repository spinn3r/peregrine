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

        ChunkReader reader = _test( makeRandomSortChunk( 50000 ) );

        Tuple last = null;

        FullTupleComparator comparator = new FullTupleComparator();
        
        while( reader.hasNext() ) {

            byte[] key = reader.key();
            byte[] value = reader.value();

            Tuple t = new Tuple( key, value );
            
            if ( last != null && comparator.compare( last, t ) > 0 )
                throw new RuntimeException();

            last = t;
            
        }
        
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

            byte[] key = LongBytes.toByteArray( i );
            
            writer.write( key, key );
            
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
