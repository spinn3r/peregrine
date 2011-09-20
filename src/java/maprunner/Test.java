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

        int nr_tuples = 100;
        
        for( int i = 0; i < nr_tuples; ++i ) {

            byte[] key = toKey( i );
            byte[] value = key;
            
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
        
    }

    private static int readTupleCount( String path ) throws IOException {

        final AtomicInteger tuples = new AtomicInteger();
        
        ChunkListener listener = new ChunkListener() {

                public void onEntry( byte[] key, byte[] value ) {
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

    public static void main( String[] args ) throws Exception {

        Test test = new Test();
        test.test1();
        
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
    
}
