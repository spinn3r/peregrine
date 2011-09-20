package maprunner;

import java.io.*;
import java.util.*;

import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;

public class Test {

    public static void main( String[] args ) throws Exception {

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

        byte b = (byte) -127;
        int v = b << 8;

        System.out.printf( "v: %s\n", v );
        
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
