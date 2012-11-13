package peregrine.misc;

public class PerfTest1 {

    public static void main( String[] args ) throws Exception {

        long max = 100000000000L;

        if ( args.length == 1 ) {
            max = Long.parseLong( args[0] );
        }

        System.out.printf( "max: %,d\n", max );
        
        long v = 0;
        
        for( long i = 0; i < max; ++i ) {
            ++v;
        }

        System.out.printf( "v: %,d\n", v );
        
    }
    
}

