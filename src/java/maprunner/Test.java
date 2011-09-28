package maprunner;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import java.security.*;
import java.lang.reflect.*;

import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.shuffle.*;
import maprunner.io.*;

public class Test {

    public void test1() throws IOException {

        remove( Config.DFS_ROOT );
        
        // try to write a bunch of chunk files .. make sure the right number of
        // keys are stored per file.

        Config.addPartitionMembership( 0, "cpu0", "cpu1" );
        Config.addPartitionMembership( 1, "cpu0", "cpu1" );

        String path = "/test1";
        
        ExtractWriter writer = new ExtractWriter( path );
        
        LocalPartitionWriter.CHUNK_SIZE = 1000000;

        int nr_tuples = 15;
        
        for( int i = 0; i < nr_tuples; ++i ) {

            String str = String.format( "%08d", i );
            
            byte[] key = new StringKey( str ).toBytes();
            byte[] value = new String( "xxxxxxxx" ).getBytes() ;
            
            writer.write( key, value );

        }

        writer.close();

        // now find all the chunks and read them to see how many values they
        // have.

        List<String> files = find( Config.DFS_ROOT );

        if ( files.size() != 4 ) {
            throw new RuntimeException( "Wrong number of chunk files" );
        }

        int nr_written_tuples = 0;
        for( String file : files ) {

            if ( file.endsWith( ".dat" ) == false )
                continue;
            
            System.out.printf( "%s\n", file );

            nr_written_tuples += readTupleCount( file );
            
        }

        System.out.printf( "nr_written_tuples: %,d\n", nr_written_tuples );

        if ( nr_written_tuples != nr_tuples * 2 ) {
            throw new RuntimeException( "Wrong number of tuples written" );
        }
        
    }

    private static int readTupleCount( String path ) throws IOException {

        final AtomicInteger tuples = new AtomicInteger();

        ChunkReader reader = new DefaultChunkReader( path );

        while( true ) {
            Tuple t = reader.read();

            if ( t == null )
                break;

            tuples.getAndIncrement();
            
        }

        return tuples.get();
    }
    
    public static void remove( String path ) {
        remove( new File( path ) );
    }

    public static void remove( File file ) {

        File[] files = file.listFiles();
        
        for ( File current : files ) {

            if ( current.isDirectory() == false ) {
                System.out.printf( "Deleting: %s\n", current.getPath() );
                current.delete();
            } else {
                remove( current );
            }
            
        }
        
    }

    public static List<String> find( String path ) {
        return find( new File( path ), null );
    }

    public static List<String> find( File file, List<String> result ) {

        if ( result == null )
            result = new ArrayList();
        
        File[] files = file.listFiles();
        
        for ( File current : files ) {

            if ( current.isDirectory() == false ) {
                result.add( current.getPath() );
            } else {
                find( current, result );
            }
            
        }

        return result;

    }

    public static Tuple[] makeTestTupleArray( int[] input ) {

        Tuple[] result = new Tuple[ input.length ];

        for( int i = 0; i < input.length; ++i ) {

            result[i] = new Tuple( new IntKey( input[i] ), new IntValue( input[i]  ) );
            
        }

        return result;
        
    }

    public static ChunkReader makeTestSortChunk( int[] input ) throws IOException {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ChunkWriter writer = new ChunkWriter( out );

        for( int i = 0; i < input.length; ++i ) {

            //byte[] hash = Hashcode.getHashcode( ""+i );
            
            //byte[] hash = new byte[8];
            //hash[7] = (byte)(i + 32);

            byte[] hash = LongBytes.toByteArray( (long)i );
            
            System.out.printf( "%d encodes as: %s\n", i, Base64.encode( hash ) );
            
            ByteArrayKey key = new ByteArrayKey( hash );

            writer.write( key.toBytes(), new IntValue( input[i] ).toBytes() );
            
        }

        writer.close();

        return new DefaultChunkReader( out.toByteArray() );
        
    }

    private static ThreadLocal local = new ThreadLocalMessageDigest( "SHA1" );

    public static void main( String[] args ) throws Exception {

        Field field = sun.misc.Unsafe.class.getDeclaredField( "theUnsafe" );
        
        System.out.printf( "FIXME: %s\n", field );
        field.setAccessible( true );

        sun.misc.Unsafe unsafe = (sun.misc.Unsafe)field.get( null );

        long ptr = unsafe.allocateMemory( 8 );

        unsafe.setMemory( ptr, 8L , (byte)0 );
        
        //System.out.printf( "ptr: %,d\n", ptr );
        
        unsafe.putLong( (long)100, ptr );
        
        //sun.misc.Unsafe.getUnsafe();
        
        //byte[][] foo = new byte[5][];

        //byte[0] = new byte[5];
        //byte[1] = new byte[4];
        
//         byte[] d =  new byte[2];
        
//         ChunkReader left  = makeTestSortChunk( new int[] { 0, 1, 2 } );
//         ChunkReader right = makeTestSortChunk( new int[] { 3, 4, 5 } );

//         List<ChunkReader> work = new ArrayList();
//         work.add( left );
//         work.add( right );
        
//         new Sorter( new SortListener() {

//                 public void onFinalValue( byte[] key, List<byte[]> values ) {

//                     /*
//                     ByteArrayListValue lv = new ByteArrayListValue();
//                     lv.fromBytes( values );
                    
//                     List<byte[]> list = lv.getValues();
//                     */
                    
//                     System.out.printf( "sorted value: key=%s, value=%s\n", Base64.encode( key ), values );

//                 }

//             } ).sort( work );

//         list.add( makeTestTupleArray( new int[] { 0 } ) );

//         MessageDigest md = (MessageDigest)local.get();        

//         long before = System.currentTimeMillis();

//         byte[] block = new byte[ 1000 ];
        
//         for( int i = 0; i < 100000; ++i ) {
//             md.update( block );
            
//         }

//         byte[] result = md.digest();

//         long last = System.currentTimeMillis();

//         System.out.printf( "duration: %,d ms\n", (last-before) );

//         List<SortRecord[]> list = new ArrayList();

//         list.add( makeTestTupleArray( new int[] { 0 } ) );
//         list.add( makeTestTupleArray( new int[] { 1 } ) );
//         list.add( makeTestTupleArray( new int[] { 2 } ) );

//         SortRecord[] records = new Sorter().sort( list );

//         System.out.printf( "length: %s\n", records.length );

        /*
        SortListener listener = new SortListener() {

            };

        listener.getClass().newInstance();
        */
        //Test test = new Test();
        //test.test1();

        // implementation of a merge sort algorithm

        // http://en.wikipedia.org/wiki/Merge_sort

        //FIXME: test with arrays of different lengths;
        
        //int[] vect_left = new int[] { 1, 3, 5, 7, 9 };
        //int[] vect_right = new int[] { 0, 2, 4, 6, 8 };

//         int[] vect_left  = new int[] { 1, 2, 3, 7 };
//         int[] vect_right = new int[] { 1, 2, 4, 6, 8 };

//         SortRecord[] result1 = MapOutputSortCallable.sort( makeTestTupleArray( vect_left ),
//                                                            makeTestTupleArray( vect_right ) );

//         System.out.printf( "---\n" );
        
//         SortRecord[] result2 = MapOutputSortCallable.sort( makeTestTupleArray( vect_left ),
//                                                            makeTestTupleArray( vect_right ) );

//         System.out.printf( "---\n" );
//         System.out.printf( "dumping result1\n" );

//         System.out.printf( "---\n" );

//         List<SortRecord[]> list = new ArrayList();
//         list.add( result1 );
//         list.add( result2 );
//         list.add( result1 );
//         list.add( result2 );
        
//         MapOutputSortCallable.sort( list );

        /*
        List<SortRecord[]> chunks = new ArrayList();
        chunks.add( result1 );
        chunks.add( result2 );
        */
        
        //result.dump();

        /*
        VarintWriter writer = new VarintWriter();

        for( int i = 0; i < 200; ++i ) {
            byte[] data = writer.write( i );

            //data = data >> 7;
            
            System.out.printf( "i=%d , len: %d , test: %d\n" , i, data.length, (data[0] >> 7) );

        }
        */

//         BulkArray array = new BulkArray();

//         for( int i = 0; i < 17000; ++i ) {
//             array.add( new Tuple( toKey( i ), new byte[0] ) );
//         }

//         Tuple[] data = array.toArray();

//         System.out.printf( "length: %,d\n", data.length );

//         for( Tuple t : data ) {

//             if ( t == null )
//                 throw new RuntimeException( "null entry" );
            
//         }

//         Arrays.sort( data );

        //long value = Math.abs( Hashcode.toLong( key_bytes ) );

//         int nr_partitions = 2;
        
//         long value = 1392215290534133405L;
//         int partition = (int)(value % nr_partitions);

//         System.out.printf( "partition: %d\n" , partition );

//         byte b = (byte) -127;
//         int v = b << 8;

//         System.out.printf( "v: %s\n", v );
        
    }

    public static byte[] toKey( int v ) {

        // this is lame but it works for the test....

        String str = String.format( "%08d", v );

        byte[] result = new byte[ 8 ];
        
        for( int i = 0; i < 8 ; ++ i ) {
            result[i] = (byte)str.charAt( i );
        }

        return result;
        
    }

    public static int toInt( byte[] data ) {
        return Integer.parseInt( new String( data ) );
    }

}
