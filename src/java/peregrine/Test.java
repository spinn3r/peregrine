package peregrine;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;
import org.jboss.netty.buffer.*;
import peregrine.config.*;
import peregrine.shuffle.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.os.*;

import org.jboss.netty.logging.*;

import org.apache.hadoop.util.*;

import com.spinn3r.log5j.Logger;

import com.sun.jna.Pointer;

public class Test {

    private static final Logger log = Logger.getLogger();

    public static void test1() {

        long before = System.currentTimeMillis();

        byte[] data = new byte[ 1000000 ];

        for( int j = 0; j < 3000; ++j ) {
            
            int val = 0;
            for( int i = 0; i < data.length; ++i ) {
                val += data[i];
            }

        }
            
        long after = System.currentTimeMillis();
        System.out.printf( "test1 duration: %,d ms\n" , (after-before) );
        
    }

    public static void test2() {

        long before = System.currentTimeMillis();

        byte[] data = new byte[ 1000000 ];

        for( int j = 0; j < 3000; ++j ) {
            
            int val = 0;
            for( int i = 0; i < data.length; ++i ) {
                val += getByte( data, i );
            }

        }
            
        long after = System.currentTimeMillis();
        System.out.printf( "test2 duration: %,d ms\n" , (after-before) );
        
    }

    public static byte getByte( byte[] data, int idx ) {
        return data[idx];
    }

    public static final int CHECKSUM_CAPACITY = 1024;
    public static final int CHECKSUM_ITERS    = 5000000;
    
    public static void test3() {

        PureJavaCrc32C checksum = new PureJavaCrc32C();

        long before = System.currentTimeMillis();

        byte[] data = new byte[ CHECKSUM_CAPACITY ];
        
        for( int i = 0; i < CHECKSUM_ITERS; ++i ) {
            checksum.update( data, 0, data.length );
        }

        long after = System.currentTimeMillis();

        long written = (long)data.length * (long)CHECKSUM_ITERS;

        long duration = (after-before);

        long throughput = (written / duration) * 1000L; 
        
        System.out.printf( "test3: Wrote %,d bytes in duration %,d ms at %,d b/s\n" , written, duration, throughput );

    }
    
    public static void test4() {

        PureJavaCrc32C checksum = new PureJavaCrc32C();

        long before = System.currentTimeMillis();
        
        ChannelBuffer buff = ChannelBuffers.buffer( CHECKSUM_CAPACITY );
        
        for( int i = 0; i < CHECKSUM_ITERS; ++i ) {
            checksum.update( buff, 0, CHECKSUM_CAPACITY );
        }

        long after = System.currentTimeMillis();

        long written = (long)CHECKSUM_CAPACITY * (long)CHECKSUM_ITERS;

        long duration = (after-before);

        long throughput = (written / duration) * 1000L; 
        
        System.out.printf( "test4: Wrote %,d bytes in duration %,d ms at %,d b/s\n" , written, duration, throughput );

    }

    public static void dump( String[] data ) {

        for( String v : data ) {
            System.out.printf( "  %s\n", v );
        }
        
    }

    public static void foo( long v ) {
        System.out.printf( "v: %s\n", v );
    }

    public static void main( String[] args ) throws Exception {

        /*
        String path = "/d0/util0029.wdc.sl.spinn3r.com/11112/1/tmp/default.1/merged-0.tmp";

        new MappedFile( path, "r" ).map();
        */

        foo( (int) 100 );

        
        /*
        List<String> list = new ArrayList();

        list.add( "foo" );
        list.add( "bar" );

        System.out.printf( "%s\n", list );

        System.out.printf( "--\n" );

        String[] array = new String[2];
        list.toArray( array );
        
        dump( array );
        */
        
        /*
        dump( (String[]) list.toArray() );
        */
        
        /*
        int capacity = 19;
        
        ChannelBuffer buff = ChannelBuffers.buffer( capacity );

        for( int i = 0; i < capacity; ++i ) {
            buff.writeByte( i );
        }

        System.out.printf( "%s\n", Hex.pretty( buff ) );

        System.out.printf( "====\n" );
        
        List<ChannelBuffer> split = CRC32ChannelBuffer.split( buff, 8 );

        for( ChannelBuffer current : split ) {
            System.out.printf( "%s\n", Hex.pretty( current ) );
        }
        */
        
        // test1();
        // test1();
        // test1();

        // test2();
        // test2();
        // test2();

        // InternalLoggerFactory.setDefaultFactory( new Log4JLoggerFactory() );

        // File file = new File( "test.mmap" );

        // MappedFile mappedFile = new MappedFile( file, FileChannel.MapMode.READ_ONLY );
        // mappedFile.setLock( true );

        // mappedFile.map();

        // mappedFile.close();

        // File file = new File( "test.mmap" );
        
        // FileInputStream in = new FileInputStream( file );

        // long length = file.length();

        // FileChannel channel = in.getChannel();
        
        // // mmap the WHOLE file. We won't actually use these pages if we don't
        // // read them so this make it less difficult to figure out what to map.
        // MappedByteBuffer map = channel.map( FileChannel.MapMode.READ_ONLY, 0, length );

        // SlabDynamicChannelBuffer buff = new SlabDynamicChannelBuffer( 1 , 1 );

        // for( int i = 0; i < 10000; ++i ) {
        //     buff.writeInt( i );
        // }

        // while( true ) {
        //     ChannelBuffer buff = ChannelBuffers.directBuffer( 2097152 );
        //     System.out.printf( "." );
        // }
        
        /*
        
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
        */

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
