package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.nio.*;
import java.nio.channels.*;
import java.lang.reflect.*;

import peregrine.os.*;
import peregrine.util.*;
import peregrine.app.pagerank.*;
import peregrine.config.*;
import peregrine.worker.*;
import peregrine.rpc.*;

import org.jboss.netty.buffer.*;

import com.sun.jna.Pointer;

import com.spinn3r.log5j.Logger;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.hadoop.*;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.db.*;

import java.nio.charset.Charset;

// needed so that we can configure the InputFormat for Cassandra
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;

public class Test {

    private static final Logger log = Logger.getLogger();

    protected static int MAX = 1000000;
    
    protected SimpleQueue<Integer> queue = null;

    public static void testlock1() throws Exception {

        Config config = new Config();

        new Initializer( config ).workerd();

        System.out.printf( "testlock1 of a large file NOT in page cache on the first mlock.\n" );

        File file = new File( "test.dat" );
        fcntl.posix_fadvise( file, 0L, file.length(), fcntl.POSIX_FADV_DONTNEED );
        
        MappedFileReader reader = new MappedFileReader( config, file );
        reader.setAutoLock( true );

        reader.map();

    }

    public static void main( String[] args ) throws Exception {

        /*
        Config config = new Config();

        new Initializer( config ).basic( "test" );

        System.out.printf( "%s\n" , new Tracepoint() );
        System.out.printf( "%s\n" , new Tracepoint("foo", "bar", "cat", "dog") );
        System.out.printf( "%s\n" , new Tracepoint( new Exception( "fake exception" ) ) );
        System.out.printf( "%s\n" , new Tracepoint( new Exception() ) );

        new Exception().printStackTrace();

        log.info( "INFO: hello world" );
        log.error( "ERROR: hello world" );
        log.error( String.format( "hello world: \n%s", new Tracepoint( new Exception( "fake exception" ) ) ) );
        log.error( "hello world: %s", new Tracepoint( new Exception( "fake exception" ) ) );
        */

        new Job().fromMessage( new Job().toMessage() );

        Host host = new Host( "localhost", 11111 );
        
        Client client = new Client();
        
        Message message = new Message();
        message.put( "action", "status" );
        
        Message result = client.invoke( host, "controller", message );

        System.out.printf( "response: %s\n", result );
        
    }

    public static void main_segfault( String[] args ) throws Exception {

        File file = new File( "test.segfault" );
        
        FileOutputStream fos = new FileOutputStream( file );
        fos.write( "hello world".getBytes() );
        fos.close();

        MappedFileReader reader = new MappedFileReader( file );
        
        ChannelBuffer buff = reader.map();

        Charset UTF8 = Charset.forName( "UTF-8" );

        String content;
        
        content = new String( buff.slice( 0, (int)file.length() ).toString( UTF8 ) );
        
        System.out.printf( "content: %s\n", content );

        reader.close();

        //This should cause us to segfault now... 
        
        content = new String( buff.slice( 0, (int)file.length() ).toString( UTF8 ) );
        
        System.out.printf( "content: %s\n", content );

    }
    
    public static void main5( String[] args ) throws Exception {

        Config config = new Config();

        config.setUser( "nobody" );

        Initializer init = new Initializer( config );

        init.setuid();

    }

    public static void main3( String[] args ) throws Exception {

        FileInputStream fis = new FileInputStream( "test.dat" );

        sun.nio.ch.FileChannelImpl channel = (sun.nio.ch.FileChannelImpl)fis.getChannel();

        System.out.printf( "FIXME: %s\n", channel.getClass().getName() );

        for( Method m : channel.getClass().getDeclaredMethods() ) {
            System.out.printf( "method: %s\n", m );
            
        }
        
    }
        
    public static void main2( String[] args ) throws Exception {

        if ( args[0].equals( "--anonymous" ) ) {

            System.out.printf( "testing anon map\n" );

            int capacity = 500000000;

            ByteBuffer buff = ByteBuffer.allocateDirect( capacity );

            //touch every page.
            for( int i = 0; i < capacity; ++i ) {
                buff.put( (byte) 0 );
            }
            
        } else if ( args[0].equals( "--mmap" ) ) {

            System.out.printf( "testing mmap\n" );

            MappedFileReader reader = new MappedFileReader( null, "test.dat" );
            reader.setAutoLock( true );

            reader.map();
            //reader.load();

        } else if ( args[0].equals( "--mmap-nolock" ) ) {

            System.out.printf( "testing mmap without lock\n" );

            MappedFileReader reader = new MappedFileReader( null, "test.dat" );
            reader.setAutoLock( false );

            reader.map();
            //reader.load();

        } else if ( args[0].equals( "--mmap-multi" ) ) {

            System.out.printf( "RLIMIT_MEMLOCK: %s\n",
                               resource.getrlimit( resource.RLIMIT.MEMLOCK ) );

            long limit = 51200000L;

            System.out.printf( "limit: %,d\n", limit );
            
            resource.setrlimit( resource.RLIMIT.MEMLOCK,
                                new resource.RlimitStruct( limit ) );

            System.out.printf( "RLIMIT_MEMLOCK: %s\n", resource.getrlimit( resource.RLIMIT.MEMLOCK ) );
            
            int max = 10;

            if ( args.length >= 2 ) {
                max = Integer.parseInt( args[1] );
            }
            
            System.out.printf( "testing mmap multiple times: %,d\n", max );

            for( int i = 0; i < max; ++i ) {

                System.out.printf( "." );

                // dd if=/dev/zero of=test.dat count=1000000

                MappedFileReader reader = new MappedFileReader( null, "test.dat" );
                reader.setAutoLock( true );
                
                reader.map();
                //reader.load();
                
            }
                
        } else {

            System.out.printf( "no test\n" );
            
        }

        System.out.printf( "\n" );
        System.out.printf( "sleeping...\n" );
        
        Thread.sleep( Long.MAX_VALUE );

        /*
        Test t = new Test();

        t.test0();
        t.test0();
        t.test0();

        t.test1();
        t.test1();
        t.test1();

        int hosts = 9;
        
        Config config = new Config( "localhost", 11112 );
        
        //config.setController( controller );
        config.setConcurrency( 2 );
        config.setReplicas( 2 );

        for( int i = 0; i < hosts; ++i ) {
            config.getHosts().add( new Host( "localhost", Host.DEFAULT_PORT + i ) );
        }
        
        config.init();
        */

    }

    public void test0() {

        System.out.printf( "test0\n" );
        queue = new FastQueue();
        test();
        
    }

    public void test1() {

        System.out.printf( "test1\n" );
        queue = new SlowQueue();
        test();
        
    }

    public void test() {

        System.gc();

        long before = System.currentTimeMillis();

        WriterThread wt = new WriterThread();
        wt.start();
        wt.waitFor();

        ReaderThread rt = new ReaderThread();
        rt.start();
        rt.waitFor();

        long after = System.currentTimeMillis();

        System.out.printf( "duration: %,d ms\n", (after-before) );
        
    }

    class WriterThread extends WaitableThread {

        public void run() {
            
            for( int i = 0; i < MAX; ++i ) {
                queue.write( i );
            }

            complete();
            
        }
        
    }

    class ReaderThread extends WaitableThread {

        public void run() {

            for( int i = 0; i < MAX; ++i ) {
                queue.read();
            }

            complete();

        }

    }

    class WaitableThread extends Thread {

        BlockingQueue<Boolean> result = new ArrayBlockingQueue( 1 );

        public void waitFor() {

            try {
                result.take();
            } catch ( Exception e ) {
                throw new RuntimeException( e );
            }

        }

        public void complete() {

            try {
                result.put( true );
            } catch ( Exception e ) {
                throw new RuntimeException( e );
            }

        }
        
    }

    class FastQueue<T> implements SimpleQueue<T> {

        int CAPACITY = 100000;

        BlockingQueue<T> writeQueue = new ArrayBlockingQueue( CAPACITY );

        BlockingQueue<T> readQueue  = new ArrayBlockingQueue( CAPACITY );

        public void write( T val ) {

            try {
                writeQueue.put( val );
            } catch ( Exception e ) {
                throw new RuntimeException( e );
            }
            
        }

        public T read() {

            try {

                T result = null;
                
                if ( readQueue.size() == 0 ) {
                    writeQueue.drainTo( readQueue );
                }

                if ( readQueue.size() > 0 ) {
                    return readQueue.take();
                } 

                return writeQueue.take();

            } catch ( Exception e ) {
                throw new RuntimeException( e );
            }

        }

    }

    class SlowQueue<T> implements SimpleQueue<T> {

        int CAPACITY = 100000;
        
        BlockingQueue<T> writeQueue = new ArrayBlockingQueue( CAPACITY );

        public void write( T val ) {

            try {
                writeQueue.put( val );
            } catch ( Exception e ) {
                throw new RuntimeException( e );
            }
            
        }

        public T read() {

            try {
                return writeQueue.take();
            } catch ( Exception e ) {
                throw new RuntimeException( e );
            }

        }

    }

    interface SimpleQueue<T> {

        public void write( T val );
        public T read();
        
    }
    
}

