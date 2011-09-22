package maprunner;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;

public class Test {

    public void test1() throws IOException {

        remove( Config.DFS_ROOT );
        
        // try to write a bunch of chunk files .. make sure the right number of
        // keys are stored per file.

        Config.addPartitionMembership( 0, "cpu0", "cpu1" );
        Config.addPartitionMembership( 1, "cpu0", "cpu1" );

        String path = "/test1";
        
        ExtractWriter writer = new ExtractWriter( path );

        PartitionWriter.CHUNK_SIZE = 1000000;

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
        
        ChunkListener listener = new ChunkListener() {

                public void onEntry( byte[] key, byte[] value ) {

                    //int kv = toInt( key );

                    System.out.printf( "kv: %s\n", new String( key ) );
                    
                    tuples.getAndIncrement();
                }

            };

        ChunkReader reader = new ChunkReader( path, listener );
        reader.read();

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

    public static void sort( int[] vect_left, int[] vect_right ) {

        SortInput left = new SortInput( vect_left );
        SortInput right = new SortInput( vect_right );

        //result index... 
        int index = 0;
        int[] result = new int[vect_left.length + vect_right.length];
        
        while( true ) {

            SortInput hit = null;
            SortInput miss = null;
            
            if ( left.value <= right.value ) {

                hit = left;
                miss = right;
                
                System.out.printf( "%d\n", hit.value );

            } else {

                hit = right;
                miss = left;
                
                System.out.printf( "%d\n", hit.value );

            }

            ++hit.idx;

            if ( hit.idx == hit.vect.length ) {
                //drain the data from the 'miss' so that there are no more
                //entries in it.

                for( int i = miss.idx; i < miss.vect.length; ++i ) {
                    System.out.printf( "%d\n", miss.vect[i] );
                }
                
                break;
            }

            hit.value = hit.vect[ hit.idx ];
            
        }

    }

    static final class SortInput {

        public int value;
        public int idx = 0;
        public int[] vect;
        
        public SortInput( int[] vect ) {
            this.vect = vect;
            this.value = vect[0];
        }

    }
    
    public static void main( String[] args ) throws Exception {

        //Test test = new Test();
        //test.test1();

        // implementation of a merge sort algorithm

        // http://en.wikipedia.org/wiki/Merge_sort

        //FIXME: test with arrays of different lengths;
        
        int[] vect_left = new int[] { 1, 3, 5, 7, 9 };
        int[] vect_right = new int[] { 0, 2, 4, 6, 8 };

        sort( vect_left, vect_right );
        
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
