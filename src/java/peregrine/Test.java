package peregrine;

import java.util.*;
import org.jboss.netty.buffer.*;
import peregrine.config.*;
import peregrine.shuffle.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

public class Test {

    private static final Logger log = Logger.getLogger();

    public static void test0() throws Exception {

        System.gc();

        System.out.printf( "-------- test ChannelBuffers.buffer\n" );

        long before = System.currentTimeMillis();

        int max = 100;

        int capacity = 1000000; 

        System.out.printf( "using capacity: %s\n", capacity );

        List<ChannelBuffer> list = new ArrayList();

        for ( int i = 0; i < max; ++i ) {
            ChannelBuffer buff = ChannelBuffers.buffer( capacity );
            list.add( buff );
        }

        long after = System.currentTimeMillis();

        System.out.printf( "duration: %,d\n", (after-before) );

    }

    public static void test1() throws Exception {

        System.gc();

        System.out.printf( "-------- test ChannelBuffers.directBuffer\n" );

        long before = System.currentTimeMillis();

        int max = 10000;

        int capacity = 16384; 

        System.out.printf( "using capacity: %s\n", capacity );

        List<ChannelBuffer> list = new ArrayList();

        for ( int i = 0; i < max; ++i ) {
            ChannelBuffer buff = ChannelBuffers.directBuffer( capacity );
            list.add( buff );
        }

        long after = System.currentTimeMillis();

        System.out.printf( "duration: %,d\n", (after-before) );

    }

    public static void main( String[] args ) throws Exception {

        /*
        
        System.setProperty( "log4j.file.name", "foo.log" );

        // init log4j ...
        org.apache.log4j.xml.DOMConfigurator.configure( "conf/log4j.xml" );

        for ( int i = 0; i < 10; ++i ) {
            log.info( "hello world" );
        }

        Thread.sleep( 10000 );
        */

        int nr_extents = Integer.parseInt( args[ 0 ] );
        int extent_size = Integer.parseInt( args[ 1 ] );

        long total_size = (long)nr_extents * (long)extent_size;
        
        System.out.printf( "Allocating %,d extents of %,d bytes for a total of %,d bytes.\n", nr_extents, extent_size, total_size );
        
        System.out.printf( "allocating.\n" );

        List<ChannelBuffer> buffs = new ArrayList();
        
        for( int i = 0; i < nr_extents; ++i ) {
            
            ChannelBuffer buff = ChannelBuffers.directBuffer( extent_size );
            buffs.add( buff );
            
        } 

        System.out.printf( "WIN\n" );
        
        //com.spinn3r.log5j.LogManager.shutdown();
        
        //
        
        // shut down the logger... 
        
        // //byte[] key = new byte[] { (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF };
        // byte[] key = new byte[] { (byte)-128, (byte)0, (byte)0,(byte)0,(byte)0,(byte)0,(byte)0,(byte)0 };

        // Config config = new Config();
        // config.setReplicas( 2 );
        // config.setConcurrency( 1 );

        // config.setHost( new Host( "localhost" ) );

        // for( int i = 0; i < 5; ++i ) {

        //     config.getHosts().add( new Host(  "localhost", Config.DEFAULT_PORT + i ) );

        // }
        
        // config.init();

        // Partition part = config.route( key );

        // System.out.printf( "part: %s for key %s with config: %s\n", part, Hex.encode( key ), config );

        /*
        
        ShuffleInputChunkReader reader
            = new ShuffleInputChunkReader( config,
                                           new Partition( 0 ),
                                           "/d2/peregrine-fs/tmp/shuffle/default/0000000000.tmp" );

        while( reader.hasNext() ) {
            reader.next();
            System.out.printf( "." );
        }
        */
        
/*
        test0();
        test0();
        test0();
        test0();

        test1();
        test1();
        test1();
        test1();
        */

    }

}
