package maprunner;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import java.security.*;

import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.shuffle.*;

public class Test1 {

    public static final int MAX = 100000;
    public static final int BLOCK_LEN = 1000;
    
    public static void test1( int buffer_size ) throws Exception {

        System.out.printf( "test1: \n" );

        System.out.printf( "buffer_size: %,d bytes\n", buffer_size );
        
        long before = System.currentTimeMillis();
        
        ByteArrayOutputStream bos;

        if ( buffer_size != -1 )
            bos = new ByteArrayOutputStream( buffer_size + 1 );
        else
            bos = new ByteArrayOutputStream();

        byte[] block = new byte[BLOCK_LEN];
        
        for (int i = 0; i < MAX; ++i ) {
            bos.write( block );
        }

        bos.close();

        byte[] result = bos.toByteArray();

        long after = System.currentTimeMillis();

        System.out.printf( "duration: %,d ms\n" , (after-before) );
        
    }

    public static void test2( int buffer_size ) throws Exception {

        System.out.printf( "test2: \n" );

        long before = System.currentTimeMillis();

        System.out.printf( "buffer_size: %,d bytes\n", buffer_size );
        byte[] b = new byte[ buffer_size ];

        b = null;

        long after = System.currentTimeMillis();

        System.out.printf( "duration: %,d ms\n" , (after-before) );

    }

    public static void main( String[] args ) throws Exception {

        // test performance , and resize ability.
        test1(-1);
        test1(-1);
        test1(-1);

        test1( BLOCK_LEN * MAX );
        test1( BLOCK_LEN * MAX );
        test1( BLOCK_LEN * MAX );

        test2( BLOCK_LEN * MAX );
        test2( BLOCK_LEN * MAX );
        test2( BLOCK_LEN * MAX );

    }

}
       
