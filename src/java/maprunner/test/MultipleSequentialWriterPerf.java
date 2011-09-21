package maprunner.test;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * 
 */
public class MultipleSequentialWriterPerf {

    public static void main( String[] args ) throws Exception {

        long size = Long.parseLong( args[0] );
        
        int max = 1;
        
        for ( int i = 0; i <= 7; ++i ) {

            System.out.printf( "Writing to %s files: %d\n", max );

            long before = System.currentTimeMillis();
            
            ExecutorService es = Executors.newCachedThreadPool() ;

            List<Future> futures = new ArrayList( max );

            long writer_size = size / max;
            
            for (int j = 0; j < max; ++j ) {
                futures.add( es.submit( new WriterClass( j, writer_size ) ) );
            }

            for ( Future future : futures ) {
                future.get();
            }

            long after = System.currentTimeMillis();

            long duration = (after - before);

            System.out.printf( "duration: %,d ms\n", duration );

            max = max * 2;

        }
        
    }

}

class WriterClass implements Callable {

    public static int BLOCK_SIZE = 16384;

    public static byte[] BLOCK = new byte[ BLOCK_SIZE ];
    
    private int index;
    private long size;
    
    public WriterClass( int index, long size ) {
        this.index = index;
        this.size = size;
    }
    
    public Object call() throws Exception {

        BufferedOutputStream out =
            new BufferedOutputStream( new FileOutputStream( "./maprunner-test-" + index ) );

        for ( int i = 0 ; i < size / BLOCK_SIZE; ++i ) {
            out.write( BLOCK );
        }

        out.close();

        return null;
        
    }

}