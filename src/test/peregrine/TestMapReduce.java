package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.pagerank.*;
import peregrine.io.partition.*;
import peregrine.pfsd.*;

public class TestMapReduce extends peregrine.BaseTestWithTwoDaemons {

    public static int MAX = 900000;
    
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
            if ( values.size() != 2 )
                throw new RuntimeException( String.format( "%s does not equal %s (%s) on nth reduce %s" , values.size(), 2, ints, nth ) );

            ++nth;
            
        }

        @Override
        public void cleanup() {

 //     FIXME: add this back in
//            if ( count == 0 )
//                throw new RuntimeException();
            
        }

    }

    public void test1() throws Exception {

        // 100000000 * 32 == 3.2GB
        // 10000000 * 32 == 320MB
        
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

        for( int i = 0; i < max; ++i ) {
            byte[] key = LongBytes.toByteArray( i );
            byte[] value = key;

            writer.write( key, value );
        }

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

        runTests();

    }

}