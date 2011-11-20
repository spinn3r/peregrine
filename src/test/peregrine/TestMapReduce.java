package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.util.primitive.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

public class TestMapReduce extends peregrine.BaseTestWithMultipleConfigs {

    private static final Logger log = Logger.getLogger();

    // TODO: test 0, 1, etc... but we will need to broadcast this value to test
    // things.

    public static int[] TESTS = { 2500, 10000 };

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

            List<Integer> ints = new ArrayList();

            for( byte[] val : values ) {
                ints.add( IntBytes.toInt( val ) );
            }

            if ( values.size() != 2 ) {

                throw new RuntimeException( String.format( "%s does not equal %s (%s) on nth reduce %s" ,
                                                           values.size(), 2, ints, count ) );
            }

            count.getAndIncrement();

        }

        @Override
        public void cleanup() {

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

    private void fsck( String path ) throws IOException {

        log.info( "Running fsck of %s", path );
        
        Membership membership = config.getMembership();
        
        for( Partition part : config.getMembership().getPartitions() ) {

            List<Host> hosts = config.getMembership().getHosts( part );

            Set state = new HashSet();

            for( Host host : hosts ) {

                try {

                    String relative = String.format( "/%s%s", part.getId(), path );
                    
                    log.info( "Checking %s on %s", relative, host );
                    
                    RemotePartitionWriterDelegate delegate = new RemotePartitionWriterDelegate();
                    delegate.init( config, part, host, relative );
                    
                    java.util.Map stat = delegate.stat();
                    
                    state.add( stat.get( "X-length" ) );

                } catch ( IOException e ) {
                    throw new IOException( "fsck failed: " , e );
                }
                
            }

            if ( state.size() != 1 )
                throw new IOException( String.format( "fsck failed.  %s is in inconsistent state: %s", part, state ) );
            
        }
        
    }
    
    private void doTest( int max ) throws Exception {

        System.gc();

        Runtime runtime = Runtime.getRuntime();
        
        long before = runtime.totalMemory() - runtime.freeMemory();
        
        String path = String.format( "/test/%s/test1.in", getClass().getName() );
        
        ExtractWriter writer = new ExtractWriter( config, path );

        for( int i = 0; i < max; ++i ) {

            byte[] key = MD5.encode( "" + i );
            byte[] value = key;

            writer.write( key, value );
        }

        // write data 2x to verify that sorting works.
        for( int i = 0; i < max; ++i ) {
            byte[] key = MD5.encode( "" + i );
            byte[] value = key;

            writer.write( key, value );
        }

        writer.close();

        fsck( path );

        // verify that the right number of items have been written to filesystem.

        Set<Partition> partitions = config.getMembership().getPartitions();

        int count = 0;

        int idx = 0;

        for( Partition part : partitions ) {

            Config part_config = configsByHost.get( config.getMembership().getHosts( part ).get( 0 ) );
            
            LocalPartitionReader reader = new LocalPartitionReader( part_config , part, path );

            int countInPartition = 0;
            
            while( reader.hasNext() ) {

                reader.key();
                reader.value();

                ++countInPartition;
                
            }

            System.out.printf( "Partition %s has entries: %,d \n", part, countInPartition );

            count += countInPartition;

        }

        assertEquals( max * 2, count );

        // the writes worked correctly.
        
        String output = String.format( "/test/%s/test1.out", getClass().getName() );

        Controller controller = new Controller( config );

        try {
            controller.map( Map.class, path );

            // make sure the shuffle output worked
            
            controller.reduce( Reduce.class, new Input(), new Output( output ) );

            System.gc();

            long after = runtime.totalMemory() - runtime.freeMemory();

            long used = after - before ;
            
            System.out.printf( "Memory footprint before = %,d bytes, after = %,d bytes, diff = %,d bytes\n",
                               before, after, used );

        } finally {
            controller.shutdown();
        }
        
    }

    public static void main( String[] args ) throws Exception {

        System.setProperty( "peregrine.config", "2:1:4" );
        
        runTests();

    }

}