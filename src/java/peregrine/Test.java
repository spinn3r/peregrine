package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.nio.*;
import java.nio.channels.*;
import java.lang.reflect.*;

import peregrine.os.*;
import peregrine.util.*;
import peregrine.util.primitive.*;
import peregrine.app.pagerank.*;
import peregrine.config.*;
import peregrine.worker.*;
import peregrine.rpc.*;
import peregrine.sort.*;

import org.jboss.netty.buffer.*;

import com.sun.jna.Pointer;

import com.spinn3r.log5j.Logger;

import java.nio.charset.Charset;

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

        Job job = new Job();

        job.getParameters().put( "foo", "bar" );

        Message message = job.toMessage();
        
        System.out.printf( "job: %s\n", message.toString() );

        job = new Job();

        job.fromMessage( message );

        System.out.printf( "foo=%s\n", job.getParameters().getString( "foo" ) );

        // byte[] b0 = new byte[] { (byte)0x00 , (byte)0x00 , (byte)0x00 , (byte)0x00 , (byte)0x00 , (byte)0x01 , (byte)0x85 , (byte)0x84 , (byte)0x6d , (byte)0x40 , (byte)0x27 , (byte)0x64 , (byte)0xd5 , (byte)0x3c , (byte)0x61 , (byte)0xe2 };

        // byte[] b1 = new byte[] { (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f };

        // List<StructReader> list = new ArrayList();

        // list.add( StructReaders.wrap( b0 ) );
        // list.add( StructReaders.wrap( b1 ) );

        // Collections.sort( list, new StrictSortDescendingComparator() );
        
        // for( StructReader current : list ) {
        //     System.out.printf( "%s\n", Hex.encode( current ) );
        // }
        
    }

    public static Map<String,Integer> partition( int offset, long[] data ) {

        System.out.printf( "offset: %s\n" , offset );
        
        int[][] buckets = new int[256][256];

        for( long i : data ){

            byte[] ba = LongBytes.toByteArray( i );

            //System.out.printf( "FIXME: %s %s\n", i , ba[] );
            
            ++buckets[ba[offset+0]][ba[offset+1]];
        }

        return filter( buckets );
    
    }

    public static Map<String,Integer> filter( int[][] buckets ) {

        Map<String,Integer> result = new TreeMap();
        
        for( int i = 0; i < 256; ++i ) {

            for( int j = 0; j < 256; ++j ) {

                int v = buckets[i][j];

                if ( v > 0 ) {
                    result.put( String.format( "%s:%s", i, j ), v );
                }
                
            }
                
        }

        return result;
        
    }
    
    public static void main20( String[] args ) throws Exception {

        int nr_hosts = 3;
        int nr_tuples = 180;

        int nr_tuples_per_host = nr_tuples / nr_hosts;

        System.out.printf( "nr_tuples_per_host: %s\n", nr_tuples_per_host );
        
        Map<Integer,List<Integer>> db = newHostIntegerListMap( nr_hosts );

        int host = 0;

        for ( int i = nr_tuples - 1; i >= 0; --i ) {

            //System.out.printf( "i: %s,  host: %s\n", i, host );

            List<Integer> values = db.get( host );

            values.add( i );

            if ( values.size() == nr_tuples_per_host && host + 1 != nr_hosts )
                ++host;

        }

        System.out.printf( "before: %s\n", db );

        // now sort the local host values

        for( int current : db.keySet() ) {
            Collections.sort( db.get( current ) );
        }

        System.out.printf( "sorted: \n" );

        dump( db );

        //FIXME: this part is fucked and I need actually ONE point not TWO
        
        // now determine the mean points... take each host and their values and
        // determine their points.

        // if there are an even number we have to take two and mean then. If
        // there are an ODD number we have to take 1 directly.

        int tuples_per_local_region = (int)Math.ceil( nr_tuples_per_host / (double)nr_hosts );

        int tuple_index = tuples_per_local_region;
        
        System.out.printf( "tuples_per_local_region: %s\n", tuples_per_local_region );
        
        Map<Integer, List<Integer>> meanPointLookup = newHostIntegerListMap( nr_hosts );

        List<Integer> points = new ArrayList();
        
        while( tuple_index < nr_tuples_per_host - 1 ) {

            System.out.printf( "tuple_index: %s\n", tuple_index );

            List<Integer> hostPoints = new ArrayList();

            for( int source_host : db.keySet() ) {
                hostPoints.add( db.get( source_host ).get( tuple_index - ( tuples_per_local_region / 2 ) ) );
            }

            System.out.printf( "hostPoints: %s\n", hostPoints );

            tuple_index += tuples_per_local_region;

            points.add( meanWithFloor( hostPoints ) );
            
        }

        System.out.printf( "points: %s\n", points );

        Map<Integer,List<Integer>> routed = newHostIntegerListMap( nr_hosts );

        // now route all the values in the db

        for( List<Integer> host_values : db.values() ) {

            for( int value : host_values ) {

                int routed_host = route( value, points );

                //System.out.printf( "routed_host: %s\n", routed_host );
                
                routed.get( routed_host ).add( value );
                
            }
            
        }

        // now sort routed

        for( List<Integer> routed_values : routed.values() ) {
            Collections.sort( routed_values );
        }

        System.out.printf( "routed: %s\n", routed );

        System.out.printf( "====\n" );

        for( int current_host = 0; current_host < nr_hosts; ++current_host ) {
            System.out.printf( "%s = %s %s\n", current_host, routed.get( current_host ).size(), routed.get( current_host ) );
        }

        // the real points are 60 and 120... should I take the MEAN of the
        // median points?  or use the median points?  Right now I take the MAX
        // which isn't right. 
        
        // for( int current_host : db.keySet() ) {

        //     List<Integer> meanPoints = meanPointLookup.get( current_host );
            
        //     for( int source_host : db.keySet() ) {
        //         meanPoints.add( db.get( source_host ).get( tuple_index ) );
        //     }

        //     tuple_index += tuples_per_local_region;
            
        // }

        // System.out.printf( "meanPointLookup: %s\n", meanPointLookup );

        // // now for each host in meanPointLookup we need to determine the mean.
        // Map<Integer,Integer> hostMeanPoints = new HashMap();

        // for ( int current_host : meanPointLookup.keySet() ) {

        //     List<Integer> meanPoints = meanPointLookup.get( current_host );

        //     int sum = 0;

        //     for( int i : meanPoints ) {
        //         sum += i;
        //     }

        //     hostMeanPoints.put( current_host, (int)Math.floor( sum / meanPoints.size() ) );
            
        // }

        // System.out.printf( "hostMeanPoints: %s\n", hostMeanPoints );

        // // now create global hostMeanPoints 

        // List<Integer> meanPoints = new ArrayList();

        // for( int current : hostMeanPoints.values() ) {

        // }

    }

    public static void dump( Map<Integer,List<Integer>> map ) {

        for( int current_host = 0; current_host < map.size(); ++current_host ) {
            System.out.printf( "%s = %s %s\n", current_host, map.get( current_host ).size(), map.get( current_host ) );
        }

    }

    public static int route( int value, List<Integer> points ) {

        int result = 0;
        
        for( int i : points ) {

            if ( value <= i )
                return result;

            ++result;
            
        }

        return points.size();
        
    }
    
    public static Map<Integer,List<Integer>> newHostIntegerListMap( int nr_hosts ) {

        Map<Integer, List<Integer>> result = new HashMap();

        for( int i = 0; i < nr_hosts; ++i ) {
            result.put( i, new ArrayList() );
        }

        return result;
        
    }
    
    public static int meanWithFloor( List<Integer> list ) {

        int sum = 0;
        
        for( int i : list ) {
            sum += i;
        }
        
        return (int)Math.floor( sum / list.size() );

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

