package peregrine;

import java.io.*;
import java.util.*;
import java.net.*;
import java.nio.*;

import org.jboss.netty.bootstrap.*;
import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.util.*;
import peregrine.io.chunk.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.shuffle.*;
import peregrine.reduce.sorter.*;
import peregrine.task.*;
import peregrine.pfs.*;
import peregrine.config.*;

public class Test {

    public static void test0() throws Exception {

        Runtime runtime = Runtime.getRuntime();
        
        System.gc() ;

        long before = runtime.totalMemory() - runtime.freeMemory();

        int max = 8000000;
        
        int[] lookup = new int[max];

        for ( int i = 0; i < lookup.length; ++i ) {
            lookup[i] = i;
        }

        System.gc() ;

        long after = runtime.totalMemory() - runtime.freeMemory();

        System.out.printf( "used: %,d\n", (after-before) );

        // prevent the GC from removing this.
        int l = lookup[10]; 

    }

    public static void test1() throws Exception {

        Runtime runtime = Runtime.getRuntime();
        
        System.gc() ;

        long before = runtime.totalMemory() - runtime.freeMemory();

        int max = 8000000;
        
        //byte[] b = new byte[8000000];

        Tuple[] t = new Tuple[max];

        for ( int i = 0; i < t.length; ++i ) {
            t[i] = new Tuple( new byte[8], new byte[8] );
        }

        System.gc() ;

        long after = runtime.totalMemory() - runtime.freeMemory();

        System.out.printf( "used: %,d\n", (after-before) );

        // prevent the GC from removing this.
        Tuple foo = t[10]; 

    }

    public static void test2() throws Exception {

        Runtime runtime = Runtime.getRuntime();
        
        System.gc() ;

        long before = runtime.totalMemory() - runtime.freeMemory();

        int max = 8000000;
        
        byte[][][] lookup = new byte[max][][];

        for ( int i = 0; i < max; ++i ) {
            //lookup[i] = new Tuple( new byte[8], new byte[8] );

            lookup[i] = new byte[2][];
            
            lookup[i][0] = new byte[8];
            lookup[i][1] = new byte[8];
        }
        
        System.gc() ;

        // keep a ref to avoid GC
        byte foo = lookup[0][0][0];
        
        long after = runtime.totalMemory() - runtime.freeMemory();

        System.out.printf( "used: %,d\n", (after-before) );

    }

    public static void test3() throws Exception {

        Runtime runtime = Runtime.getRuntime();
        
        System.gc() ;

        long before = runtime.totalMemory() - runtime.freeMemory();

        int max = 8000000;

        int capacity = max * (4 + 8 + 4 + 8); 

        ChannelBuffer buff = ChannelBuffers.buffer( capacity );

        System.gc() ;

        buff.getByte(0);
        
        long after = runtime.totalMemory() - runtime.freeMemory();

        System.out.printf( "used: %,d\n", (after-before) );

    }

    public static void test4() throws Exception {

        System.gc();

        System.out.printf( "-------- test4\n" );

        long before = System.currentTimeMillis();

        int max = 4000000;

        int capacity = max * (4 + 8 + 4 + 8); 

        System.out.printf( "using capacity: %s\n", capacity );
        
        ChannelBuffer buff = ChannelBuffers.buffer( capacity );

        for ( int i = 0; i < max; ++i ) {
            buff.writeInt( 8 );
            buff.writeBytes( new byte[8] );
            buff.writeInt( 8 );
            buff.writeBytes( new byte[8] );
        }

        for ( int i = 0; i < max; ++i ) {
            buff.readInt();
            buff.readBytes( new byte[8] );
            buff.readInt();
            buff.readBytes( new byte[8] );
        }

        long after = System.currentTimeMillis();

        System.out.printf( "duration: %,d\n", (after-before) );

    }

    // public static void test5() throws Exception {

    //     System.gc();
        
    //     System.out.printf( "-------- test5\n" );
        
    //     long before = System.currentTimeMillis();

    //     int max = 4000000;

    //     int capacity = max * (4 + 8 + 4 + 8); 

    //     //ChannelBuffer buff = new ExtendedChannelBuffer( (int)DefaultPartitionWriter.CHUNK_SIZE );
    //     //ChannelBuffer buff = new ExtendedChannelBuffer( capacity , 4194304 );

    //     ChannelBuffer buff = new ExtendedChannelBuffer( 4194304 , 4194304 );

    //     for ( int i = 0; i < max; ++i ) {
    //         buff.writeInt( 8 );
    //         buff.writeBytes( new byte[8] );
    //         buff.writeInt( 8 );
    //         buff.writeBytes( new byte[8] );
    //     }

    //     for ( int i = 0; i < max; ++i ) {
    //         buff.readInt();
    //         buff.readBytes( new byte[8] );
    //         buff.readInt();
    //         buff.readBytes( new byte[8] );
    //     }

    //     long after = System.currentTimeMillis();

    //     System.out.printf( "duration: %,d\n", (after-before) );

    // }

    public static void test6() throws Exception {

        System.gc();
        
        System.out.printf( "-------- test6\n" );
        
        long before = System.currentTimeMillis();

        ChannelBuffer buff = ChannelBuffers.buffer( (int)DefaultPartitionWriter.CHUNK_SIZE );

        long after = System.currentTimeMillis();

        System.out.printf( "duration: %,d\n", (after-before) );

    }

    public static void test7() throws Exception {

        System.gc();
        
        System.out.printf( "-------- test7\n" );
        
        long before = System.currentTimeMillis();

        int nr_regions = 100;

        int capacity = (int)DefaultPartitionWriter.CHUNK_SIZE / nr_regions;

        for( int i = 0; i < nr_regions; ++i ) {
            ChannelBuffer buff = ChannelBuffers.buffer( capacity );
        }

        long after = System.currentTimeMillis();

        System.out.printf( "duration: %,d\n", (after-before) );

    }

    public static void test8() throws Exception {

        System.gc();
        
        System.out.printf( "-------- test8 (SHA1)\n" );

        byte[] data = new byte[10048576];
        
        long before = System.currentTimeMillis();

        for( int i = 0; i < MAX; ++i ) {
            SHA1.encode( data );
        }
        
        long after = System.currentTimeMillis();

        System.out.printf( "duration: %,d\n", (after-before) );

    }

    public static void test9() throws Exception {

        System.gc();
        
        System.out.printf( "-------- test9 (MD5)\n" );

        byte[] data = new byte[10048576];
        
        long before = System.currentTimeMillis();

        for( int i = 0; i < MAX; ++i ) {
            MD5.encode( data );
        }
        
        long after = System.currentTimeMillis();

        System.out.printf( "duration: %,d\n", (after-before) );

    }

    public static int MAX = 100;

    public static void layout0() throws Exception {

        Config config = new Config();

        config.setConcurrency( 2 );
        config.setReplicas( 2 );
        
        Set<Host> hosts = new HashSet();

        int nr_hosts = 5;
        
        for( int i = 0; i < nr_hosts; ++i ) {
            hosts.add( new Host( String.format( "node%02d", i ) ) );
            
        }

        config.setHosts( hosts );
        
        PartitionLayoutEngine layout = new PartitionLayoutEngine( config );
        layout.build();
        
        Membership membership = layout.toMembership();

        System.out.printf( "%s\n", membership.toMatrix() );

    }

    public static void layout1() throws Exception {

        Config config = new Config();

        config.setConcurrency( 4 );
        config.setReplicas( 3 );
        
        Set<Host> hosts = new HashSet();

        int nr_hosts = 16;
        
        for( int i = 0; i < nr_hosts; ++i ) {
            hosts.add( new Host( String.format( "node%02d", i ) ) );
            
        }

        config.setHosts( hosts );

        PartitionLayoutEngine layout = new PartitionLayoutEngine( config );
        layout.build();
        
        Membership membership = layout.toMembership();

        System.out.printf( "%s\n", membership.toMatrix() );

    }

    public static void main( String[] args ) throws Exception {

        org.apache.log4j.xml.DOMConfigurator.configure( "conf/log4j.xml" );

        layout0();

        System.out.printf( "====================\n" );
        
        layout1();

        // test8();
        // test8();
        // test8();

        // test9();
        // test9();
        // test9();

        // RemoteChunkWriterClient client = new RemoteChunkWriterClient( "http://localhost:11112/foo/bar" );

        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );
        // client.write( "hello world".getBytes() );

        // client.close();

        // prevent the GC from removing this.

        // FileInputStream fis = new FileInputStream( "./test.txt" );

        // System.out.printf( "%s\n", Native.getFd( fis.getFD() ) );

        // long length = 5;
        // int fd = Native.getFd( fis.getFD() );
        
        // long result = mman.mmap( 0, length, mman.PROT_READ, mman.MAP_SHARED | mman.MAP_LOCKED, fd, 0 );

        // if ( result == -1 ) {
        //     System.out.printf( "FIXME: %s\n", errno.strerror() );
        // }
        
        // System.out.printf( "result: %s\n", result );
        
        // int size = 4;

        // DefaultChunkWriter writer = new DefaultChunkWriter( new File( "/tmp/test.chunk" ) ); 

        // for( int i = 0; i < size; ++i ) {

        //     byte[] key = LongBytes.toByteArray( i );

        //     // set the value to 'x' so that I don't accidentally read the key.
        //     byte[] value = new byte[] { (byte)'x', (byte)'x', (byte)'x', (byte)'x', (byte)'x', (byte)'x', (byte)'x' };
            
        //     writer.write( key, value );
            
        // }

        // writer.close();

        // Config config = new Config();
        
        // ChunkSorter sorter = new ChunkSorter( config ,
        //                                       new Partition( 0 ),
        //                                       new ShuffleInputReference( "default" ) );

        // sorter.sort( new File( "/tmp/test.chunk" ), new File( "/tmp/test.out" ) );

        // int size = 4;
        
        // ChannelBuffer buff = ChannelBuffers.buffer( size * LongBytes.LENGTH );

        // KeyLookup lookup = new KeyLookup( size, buff );

        // int idx = 0;

        // for( int i = 0; i < size; ++i ) {
        //     buff.writeLong( i );
        // }
        
        // while( lookup.hasNext() ) {
        //     lookup.next();
        //     lookup.set( idx );
        //     idx += LongBytes.LENGTH;
        // }

        // lookup.reset();

        // lookup.dump( "test" );

        // //lookup.slice( 2, 3 ).dump("asdf");

        // lookup.slice( 0, 0 ).dump( "[0,0]" );
        // lookup.slice( 1, 1 ).dump( "[1,1]" );
        // //lookup.slice( 5, 9 ).dump( "[5,9]" );

        // Config config = new Config();
        
        // ChunkSorter sorter = new ChunkSorter( config ,
        //                                       new Partition( 0 ),
        //                                       new ShuffleInputReference( "default" ) );

        // KeyLookup result = sorter.sort( lookup );

        // result.dump( "result" );
        
        // test6();
        // test7();
        // test6();
        // test7();
        // test6();
        // test7();
        // test6();
        // test7();
        // test6();
        // test7();
        // test6();
        // test7();

        //test0();
        
        // test4();
        // test5();

        // test4();
        // test5();

        // test4();
        // test5();

        // test4();
        // test5();

        // test4();
        // test5();

        // test4();
        // test5();

        //System.out.printf( "%s\n", Hex.encode( new byte[] { (byte)-127 } ) );

        /*
        org.apache.log4j.xml.DOMConfigurator.configure( "conf/log4j.xml" );

        Config config = new Config();
        
        //ShuffleInputChunkReader reader
        //    = new ShuffleInputChunkReader( "/tmp/peregrine-fs/localhost/11112/tmp/shuffle/default/0000000000.tmp", 0 );

        // int count = 0;
        // while( reader.hasNext() ) {

        //     reader.key();
        //     reader.value();
            
        //     ++count;
            
        // }

        // System.out.printf( "count: %,d\n", count );
        
        ShuffleInputChunkReader reader = new ShuffleInputChunkReader( "/tmp/peregrine-fs/localhost/11112/tmp/shuffle/default/0000000000.tmp", 0 );

        System.out.printf( "size: %,d\n", reader.size() );

        ChunkSorter2 sorter = new ChunkSorter2( config , new Partition( 0 ), new ShuffleInputReference( "default" ) );

        sorter.sort( reader );

        DefaultChunkReader chunkReader = new DefaultChunkReader( new File( "/tmp/peregrine-fs/localhost/11112/0/tmp/default/sort-0.tmp" ) );

        int count = 0;
        while( chunkReader.hasNext() ) {

            byte[] key    = chunkReader.key();
            byte[] value  = chunkReader.value();

            System.out.printf( "%s\n", Hex.encode( key ) );
            
            ++count;
            
        }
        
        System.out.printf( "count: %s\n", count );
        */
        
        //test4();
        //test5();
        // test5();
        // test5();
        // test5();
        // test5();

        //test2();
        //test3();
        
        // 76 bytes per entry!!!!!!!!!!!!!!

        /*
        File file = new File( "conf/peregrine.hosts" );
        FileInputStream fis = new FileInputStream( file );

        byte[] data = new byte[ (int)file.length() ];
        fis.read( data );

        String[] lines = new String( data ).split( "\n" );

        for( String line : lines ) {
            System.out.printf( "line '%s'\n", line );
        }
        */
        
        /*
        List<Host> hosts = new ArrayList();

        hosts.add( new Host( "util0.wdc.sl.spinn3r.com" ) );
        hosts.add( new Host( "util1.wdc.sl.spinn3r.com" ) );
        hosts.add( new Host( "util2.wdc.sl.spinn3r.com" ) );
        hosts.add( new Host( "util3.wdc.sl.spinn3r.com" ) );
        hosts.add( new Host( "util4.wdc.sl.spinn3r.com" ) );
        hosts.add( new Host( "util5.wdc.sl.spinn3r.com" ) );
        hosts.add( new Host( "util6.wdc.sl.spinn3r.com" ) );
        hosts.add( new Host( "util7.wdc.sl.spinn3r.com" ) );
        hosts.add( new Host( "util8.wdc.sl.spinn3r.com" ) );
        hosts.add( new Host( "util9.wdc.sl.spinn3r.com" ) );

        PartitionLayoutEngine engine = new PartitionLayoutEngine( 4, 2, hosts );
        engine.build();

        Membership membership = engine.toMembership();
        
        System.out.printf( "%s\n", membership.toMatrix() );

        */
        
        // // dump the matrix
        // for( int i = 0; i < nr_hosts; ++i ) {
        //     Host host = getHost( i );

        //     System.out.printf( "%10s: ", host.getName() );

        //     List<Partition> partitions = matrix.get( host );

        //     for( Partition part : partitions ) {

        //         System.out.printf( "%10s", part.getId() );

        //     }
            
        //     System.out.printf( "\n" );
            
        // }

    }

    public static Host getHost( int i ) {
        return new Host( "host" + i );
    }
    
}

class RpcMessage {

    public String action;
    
}

class RpcMapperMessage extends RpcMessage {

    public Input input = new Input();
    
}