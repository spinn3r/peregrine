package peregrine;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.buffer.*;
import peregrine.config.*;
import peregrine.shuffle.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.os.*;

import org.jboss.netty.logging.*;

import org.apache.hadoop.util.*;

import org.apache.log4j.xml.DOMConfigurator;

import com.spinn3r.log5j.Logger;

import com.sun.jna.Pointer;

public class Test {

    private static final Logger log = Logger.getLogger();

    public static void test2() {

    }

    public static void test1( int max ) {

        long before = System.currentTimeMillis();
        
        for( int i = 0; i < max; ++i ) {
            test2();
        }

        long after = System.currentTimeMillis();

        System.out.printf( "duration: %,d ms\n", (after-before) );
        
    }
    
    public static void main( String[] args ) throws Exception {

        System.out.printf( "%s\n", resource.getrlimit( resource.RLIMIT_NOFILE ) );

        
   //      test1( Integer.parseInt( args[0] ) );

//         test1( Integer.parseInt( args[0] ) );
// com
        /*
        testQueue( new LinkedBlockingQueue( capacity ) );
        testQueue( new LinkedBlockingQueue( capacity ) );
        testQueue( new LinkedBlockingQueue( capacity ) );

        testQueue( new ArrayBlockingQueue( capacity ) );
        testQueue( new ArrayBlockingQueue( capacity ) );
        testQueue( new ArrayBlockingQueue( capacity ) );
        */
        
        // String result = format( (int)10000000, (double)1000000.0, (long)100000 );

        // System.out.printf( "%s\n" , result );

        // DOMConfigurator.configure( "conf/log4j.xml" );

        // MappedFile.FORCE_PAGE_SIZE=4096;
        // testDiskThroughput(args);

        // MappedFile.FORCE_PAGE_SIZE=16384;
        // testDiskThroughput(args);

        // MappedFile.FORCE_PAGE_SIZE=32768;
        // testDiskThroughput(args);

        /*
        FileInputStream fis = new FileInputStream( "test.txt" );

        FileChannel channel = fis.getChannel();

        int fd = Native.getFd( fis.getFD() );

        long offset = 0;
        long length = 5;

        long count = 50000;
        
        mman.mmap( length, mman.PROT_READ, mman.MAP_SHARED | mman.MAP_LOCKED, fd, offset );
        fcntl.posix_fadvise(fd, offset, length, fcntl.POSIX_FADV_WILLNEED );

        long before = System.currentTimeMillis();
        
        for( int i = 0; i < count; ++i ) {

            mman.mmap( length, mman.PROT_READ, mman.MAP_SHARED | mman.MAP_LOCKED, fd, offset );
            fcntl.posix_fadvise(fd, offset, length, fcntl.POSIX_FADV_WILLNEED );

        }

        long after = System.currentTimeMillis();

        long duration = (after-before);

        long throughput = (duration / count);

        System.out.printf( "count: %,d , duration: %,d ms , throughput: %,d per ms\n", count, duration, throughput );
        */
        
        /*
        String path = "/d0/util0029.wdc.sl.spinn3r.com/11112/1/tmp/default.1/merged-0.tmp";

        new MappedFile( path, "r" ).map();
        */

        //foo( (int) 100 );

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
