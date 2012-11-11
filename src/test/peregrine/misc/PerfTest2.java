package peregrine.misc;

import java.util.*;

public class PerfTest2 {

    public static void main( String[] args ) throws Exception {

        long max = 1000L;
        int range = 1000;

        if ( args.length == 2 ) {
            max   = Long.parseLong( args[0] );
            range = Integer.parseInt( args[1] );
        }
        
        System.out.printf( "max=%,d, range=%,d\n", max, range );

        long v = 0;

        byte[] data = new byte[range];

        Random r = new Random();
        
        for( long i = 0; i < max; ++i ) {

            int idx = r.nextInt( range );

            ++data[idx];
            
        }

    }
    
}