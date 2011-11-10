package peregrine;

import java.util.*;
import peregrine.io.*;
import peregrine.util.primitive.*;

public class TestMapReduce extends peregrine.BaseTestWithMultipleConfigs {

    public static int MAX = 90000;

    // FIXME: 0 and 1 both break 
    //public static int MAX = 2;

    // test with odd numbers tooo... 3, 9 , etc.
    public static int[] TESTS = { 0, 1, 2, 4, 8, 16, 32, 64 };
    
    public static class Map extends Mapper {

        @Override
        public void map( byte[] key,
                         byte[] value ) {

            emit( key, value );
            
        }

    }

    public static class Reduce extends Reducer {

        int count = 0;

        int nth = 0;
        
        @Override
        public void reduce( byte[] key, List<byte[]> values ) {

            ++count;

            List<Integer> ints = new ArrayList();

            for( byte[] val : values ) {
                ints.add( IntBytes.toInt( val ) );
            }
            
            // full of fail...
            /*
              FIXME: add this back in
            if ( values.size() != 2 )
                throw new RuntimeException( String.format( "%s does not equal %s (%s) on nth reduce %s" , values.size(), 2, ints, nth ) );
            */

            ++nth;
            
        }

        @Override
        public void cleanup() {

           if ( count == 0 )
               throw new RuntimeException();
            
        }

    }

    @Override
    public void doTest() throws Exception {
        doTest( MAX );
    }

    private void doTest( int max ) throws Exception {

        System.gc();

        Runtime runtime = Runtime.getRuntime();
        
        long before = runtime.totalMemory() - runtime.freeMemory();
        
        String path = String.format( "/test/%s/test1.in", getClass().getName() );
        
        ExtractWriter writer = new ExtractWriter( config, path );

        for( int i = 0; i < max; ++i ) {

            byte[] key = LongBytes.toByteArray( i );
            byte[] value = key;

            writer.write( key, value );
        }

        /*

        // FIXME add this back in.
          
        // write data 2x to verify that sorting works.
        for( int i = 0; i < max; ++i ) {
            byte[] key = LongBytes.toByteArray( i );
            byte[] value = key;

            writer.write( key, value );
        }
        */

        writer.close();
        
        String output = String.format( "/test/%s/test1.out", getClass().getName() );

        Controller controller = new Controller( config );

        // FIXME: flag the mapper and reducer to verify that the right number of
        // keys were read.
        
        controller.map( Map.class, path );
        controller.reduce( Reduce.class, new Input(), new Output( output ) );

        System.gc();

        long after = runtime.totalMemory() - runtime.freeMemory();

        long used = after - before ;
        
        System.out.printf( "Memory footprint before = %,d bytes, after = %,d bytes, diff = %,d bytes\n", before, after, used );
        
        controller.shutdown();
        
    }

    public static void main( String[] args ) throws Exception {

        if ( args.length > 0 )
            MAX = Integer.parseInt( args[0] );

        /*
        BaseTestWithMultipleConfigs.CONCURRENCY  = new int[] { 2 };
        BaseTestWithMultipleConfigs.REPLICAS     = new int[] { 1 };
        BaseTestWithMultipleConfigs.HOSTS        = new int[] { 1 };
        */
        
        runTests();

    }

}