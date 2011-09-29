package maprunner.test;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import maprunner.*;
import maprunner.io.*;
import maprunner.util.*;
import maprunner.shuffle.*;

/**
 * 
 */
public class MemorySortPerf {

    public static void test( String[] args ) throws Exception {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ChunkWriter writer = new ChunkWriter( bos );
        
        int max = Integer.parseInt( args[0] );

        Random r = new Random();
        
        int nr_random = 1000;
        
        List<byte[]> random = new ArrayList();
        
        for( int i = 0; i < nr_random; ++i ) {
            random.add( LongBytes.toByteArray( r.nextLong() ) );
        }
        
        Iterator<byte[]> it = random.iterator();
        
        for( int i = 0; i < max; ++i ) {
            
            if ( ! it.hasNext() ) {
                it = random.iterator();
            }
            
            byte[] key = it.next();
            
            writer.write( key, key );
            
        }
        
        writer.close();

        ChunkReader reader = new DefaultChunkReader( bos.toByteArray() );

        ChunkSorter sorter = new ChunkSorter();

        bos = new ByteArrayOutputStream();
        writer = new ChunkWriter( bos );
        
        long before, after;
        
        before = System.currentTimeMillis();

        sorter.sort( reader );

        System.gc();

        after = System.currentTimeMillis();
        
        System.out.printf( "duration: %,d ms\n", (after-before) );
        
    }
    
    public static void main( String[] args ) throws Exception {

        test( args );

        long before = System.currentTimeMillis();

        test( args );
        test( args );
        test( args );
        test( args );
        test( args );

        long after = System.currentTimeMillis();
        
        System.out.printf( "mean duration: %,d ms\n", ((after-before) / 5 ));

    }
    
}
