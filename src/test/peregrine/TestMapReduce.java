package peregrine;

import java.util.*;
import java.util.concurrent.atomic.*;
import peregrine.io.*;
import peregrine.util.primitive.*;

public class TestMapReduce extends peregrine.BaseTestWithMultipleConfigs {

    // TODO: test 0, 1, etc... but we will need to broadcast this value to test
    // things.
    public static int[] TESTS = { 1000 };

    public static class Map extends Mapper {

        @Override
        public void map( byte[] key,
                         byte[] value ) {

            emit( key, value );
            
        }

    }

    public static class Reduce extends Reducer {

        AtomicInteger count = new AtomicInteger();
        
        @Override
        public void reduce( byte[] key, List<byte[]> values ) {

            System.out.printf( "FIXME: in reduce... \n" );
            
            List<Integer> ints = new ArrayList();

            for( byte[] val : values ) {
                ints.add( IntBytes.toInt( val ) );
            }

            System.out.printf( "FIXME: after values\n" );
            
            if ( values.size() != 2 ) {

                System.out.printf( "FIXME: wrong number of values... ouch!\n" );
                
                throw new RuntimeException( String.format( "%s does not equal %s (%s) on nth reduce %s" ,
                                                           values.size(), 2, ints, count ) );
            }

            count.getAndIncrement();

            System.out.printf( "FIXME: count is now: %s\n", count.get() );

            System.out.printf( "FIXME: in reduce for object: %s\n", toString() );

        }

        @Override
        public void cleanup() {

            System.out.printf( "FIXME: in cleanup for object: %s\n", toString() );
            
            if ( count.get() == 0 )
               throw new RuntimeException( "count is zero" );
            
        }

    }

    @Override
    public void doTest() throws Exception {

        for( int test : TESTS ) {
            doTest( test );
        }
        
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

        // write data 2x to verify that sorting works.
        for( int i = 0; i < max; ++i ) {
            byte[] key = LongBytes.toByteArray( i );
            byte[] value = key;

            writer.write( key, value );
        }

        writer.close();
        
        String output = String.format( "/test/%s/test1.out", getClass().getName() );

        Controller controller = new Controller( config );
        
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
            TESTS = new int[] { Integer.parseInt( args[0] ) };

        BaseTestWithMultipleConfigs.CONCURRENCY  = new int[] { 2 };
        BaseTestWithMultipleConfigs.REPLICAS     = new int[] { 1 };
        BaseTestWithMultipleConfigs.HOSTS        = new int[] { 1 };
        
        runTests();

    }

}