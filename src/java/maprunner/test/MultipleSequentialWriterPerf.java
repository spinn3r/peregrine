package maprunner.test;

import java.io.*;
import java.util.*;

/**
 * 
 */
public class MultipleSequentialWriterPerf {

    public static void main( String[] args ) throws Exception {

        int max = 200;

        if ( args.length == 1 )
            max = Integer.parseInt( args[0] );

        System.out.printf( "Writing to max files: %d\n", max );
        
        for (int i = 0; i < max; ++i ) {

            new WriterClass( i ).start();
            
        }
        
    }

}

class WriterClass extends Thread {

    public static long MAX = 100000000000L;

    public static int CHUNK_SIZE = 16384;

    public static byte[] CHUNK = new byte[ CHUNK_SIZE ];
    
    private int index;
    
    public WriterClass( int index ) {
        this.index = index;
    }
    
    public void run() {

        try {
            
            BufferedOutputStream out =
                new BufferedOutputStream( new FileOutputStream( "./maprunner-test-" + index ) );

            for ( int i = 0 ; i < MAX / CHUNK_SIZE; ++i ) {
                out.write( CHUNK );
            }

            out.close();

        } catch ( Exception e ) {
            e.printStackTrace();
        }
            
    }

}