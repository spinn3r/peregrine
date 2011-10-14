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
import peregrine.pfsd.shuffler.*;

public class TestNewReduceCode extends peregrine.BaseTest {

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

            if ( count == 0 )
                throw new RuntimeException();
            
        }

    }

    protected Config config;

    protected List<FSDaemon> daemons = new ArrayList();
    
    public void setUp() {

        super.setUp();
        
        Config config0 = newConfig( "localhost", 11112 );
        //Config config1 = newConfig( "localhost", 11113 );

        daemons.add( new FSDaemon( config0 ) );
        //new FSDaemon( config1 );

        config = config0;
        
    }

    private Config newConfig( String host, int port ) {

        Config config = new Config( host, port );

        config.addPartitionMembership( 0, new Host( "localhost", 11112 ) );
        //config.addPartitionMembership( 1, new Host( "localhost", 11113 ) );

        return config;
        
    }
    
    public void test1() throws Exception {

        String path = String.format( "/test/%s/test1.in", getClass().getName() );

        DefaultPartitionWriter.CHUNK_SIZE = 16384;
        
        ExtractWriter writer = new ExtractWriter( config, path );

        int max = 10000;
        
        for( int i = 0; i < max; ++i ) {

            byte[] key = new IntKey( i ).toBytes();
            byte[] value = key;
            writer.write( key, value );
            
        }

        for( int i = 0; i < max; ++i ) {

            byte[] key = new IntKey( i ).toBytes();
            byte[] value = key;
            writer.write( key, value );
            
        }

        writer.close();
       
        String output = String.format( "/test/%s/test1.out", getClass().getName() );

        Controller controller = new Controller( config );
        
        controller.map( Map.class, path );

        for( FSDaemon daemon : daemons ) {
            daemon.shufflerFactory.closeAll();
        }

        // now see if I can reduce over the output data.

        String shuffle_file = "/tmp/peregrine-dfs/localhost/11112/0/shuffle/default-0.tmp";

        ShuffleInputChunkReader chunkReader = new ShuffleInputChunkReader( shuffle_file, 0 );

        int count = 0;
        
        while( chunkReader.hasNext() ) {

            byte[] key = chunkReader.key();
            byte[] value = chunkReader.value();

            ++count;
            
        }

        System.out.printf( "Read: %,d entries\n", count );

        assertEquals( 20000, count );
        
    }

    public static void main( String[] args ) throws Exception {
        TestNewReduceCode test = new TestNewReduceCode();
        test.setUp();
        test.test1();

        Thread.sleep( 5000L) ;
        
    }

}