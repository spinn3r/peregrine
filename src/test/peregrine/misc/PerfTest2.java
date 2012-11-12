package peregrine.misc;

import java.util.*;

public class PerfTest2 {

    public static void main( String[] args ) throws Exception {

        long max = 1000L;
        int slabs = 1;
        int range = 1000;

        if ( args.length == 2 ) {
            max   = Long.parseLong( args[0] );
            slabs = Integer.parseInt( args[1] );
            range = Integer.parseInt( args[2] );
        }
        
        System.out.printf( "max=%,d, slabs=%,d range=%,d\n", max, slabs, range );

        long v = 0;

        byte[][] data = new byte[slabs][range];

        Random r = new Random();
        
        for( long i = 0; i < max; ++i ) {

            int slab_idx = r.nextInt( slabs );
            int range_idx = r.nextInt( range );

            ++data[slab_idx][range_idx];
            
        }

    }
    
}