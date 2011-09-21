package maprunner.test;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * 
 */
public class MultipleSequentialWriterPerf {

    public static void main( String[] args ) throws Exception {

        int max = 1;
        
        for ( int i = 0; i <= 7; ++i ) {

            max = max * 2;

            System.out.printf( "Writing to max files: %d\n", max );

            long before = System.currentTimeMillis();
            
            ExecutorService es = Executors.newCachedThreadPool() ;

            List<Future> futures = new ArrayList( max );
            
            for (int j = 0; j < max; ++j ) {
                futures.add( es.submit( new WriterClass( j ) ) );
            }

            for ( Future future : futures ) {
                future.get();
            }

            long after = System.currentTimeMillis();

            long duration = (after - before);

            System.out.printf( "duration: %,d ms\n", duration );
            
        }
        
    }

}

class WriterClass implements Callable {

    public static long MAX = 100000000000L;

    public static int CHUNK_SIZE = 16384;

    public static byte[] CHUNK = new byte[ CHUNK_SIZE ];
    
    private int index;
    
    public WriterClass( int index ) {
        this.index = index;
    }
    
    public Object call() throws Exception {

        BufferedOutputStream out =
            new BufferedOutputStream( new FileOutputStream( "./maprunner-test-" + index ) );

        for ( int i = 0 ; i < MAX / CHUNK_SIZE; ++i ) {
            out.write( CHUNK );
        }

        out.close();

        return null;
        
    }

}