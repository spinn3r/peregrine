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
import maprunner.io.*;

public class TestChunkSorter {

    /**
     * test running with two lists which each have different values.
     */
    public void test1() throws Exception {

        ChunkReader reader = _test( makeRandomSortChunk( 200 ) );

        Tuple last = null;

        FullTupleComparator comparator = new FullTupleComparator();
        
        while( true ) {

            Tuple t = reader.read();

            if ( t == null )
                break;

            System.out.printf( "." );
            
            if ( last != null && comparator.compare( last, t ) > 0 )
                throw new RuntimeException();

            last = t;
            
        }
        
    }

    private ChunkReader _test( ChunkReader reader ) throws Exception {

        ChunkSorter sorter = new ChunkSorter();

        ChunkReader result = sorter.sort( reader );

        return result;
        
    }

    public static void main( String[] args ) throws Exception {

        TestChunkSorter t = new TestChunkSorter();

        t.test1();
        
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
        ChunkWriter writer = new ChunkWriter( out );

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
