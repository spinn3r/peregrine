package peregrine.perf;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.util.*;
import peregrine.shuffle.*;
import peregrine.io.partition.*;
import peregrine.io.chunk.*;
import peregrine.values.*;

/**
 * 
 */
public class ChunkWriterPerf {

    public static void perf1() throws Exception {

        System.gc();
        
        System.out.printf( "=============\n" );
        System.out.printf( "perf1 (DefaultChunkWriter using real StructWriters\n" );
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream( 100000000 ) ;

        long before = System.currentTimeMillis();
        
        DefaultChunkWriter writer = new DefaultChunkWriter( bos );

        int max = 6000000;

        for( int i = 0; i < max; ++i ) {

            byte[] key = new StructWriter()
                .writeVarint( i )
                .toBytes()
                ;

            byte[] value = key;

            writer.write( key, value );

        }

        writer.close();

        long after = System.currentTimeMillis();

        long duration = (after-before);
        
        System.out.printf( "Wrote %,d bytes in %,d ms\n", bos.size(), duration ); 

    }

    public static void perf5() throws Exception {

        System.gc();

        FileOutputStream fos = new FileOutputStream( "/dev/null" ) ;

        ByteBuffer buff = ByteBuffer.allocate( 8192 );
        buff.mark();
        
        System.out.printf( "=============\n" );
        System.out.printf( "perf5 rolling ByteBuffer being written to to /dev/null with values being written directly to the target buffer.\n" );

        long before = System.currentTimeMillis();
        
        //DefaultChunkWriter writer = new DefaultChunkWriter( out );

        int max = 6000000;

        byte[] key = new byte[8];
        byte[] value = new byte[16];

        int written = 0;

        VarintWriter varintWriter = new VarintWriter();
        
        for( int i = 0; i < max; ++i ) {

            // decide if we should flush now ...

            int width = key.length + value.length + 2;

            if ( buff.position() + width >= buff.limit() ) {

                written += buff.position();
                
                fos.write( buff.array() );
                buff.reset();

            }
            
            //out.write( (byte)0 );
            //buff.put( (byte)key.length );

            varintWriter.write( buff, key.length );
            varintWriter.write( buff, i );
            
            //buff.put( key );

            //buff.put( (byte)value.length );
            varintWriter.write( buff, value.length );
            //buff.put( value );
            varintWriter.write( buff, i );

        }

        fos.close();

        long after = System.currentTimeMillis();

        long duration = (after-before);
        
        System.out.printf( "Wrote %,d bytes in %,d ms\n", written, duration ); 

    }

    
    public static void perf2() throws Exception {

        System.gc();

        System.out.printf( "=============\n" );
        System.out.printf( "perf2\n" );
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream( 100000000 ) ;
        BufferedOutputStream buff = new BufferedOutputStream( bos, 8192 );

        OutputStream out = buff;
        
        long before = System.currentTimeMillis();
        
        //DefaultChunkWriter writer = new DefaultChunkWriter( out );

        int max = 40000004;
        
        for( int i = 0; i < max; ++i ) {

            out.write( (byte)0 );

        }

        out.close();

        long after = System.currentTimeMillis();

        long duration = (after-before);
        
        System.out.printf( "Wrote %,d bytes in %,d ms\n", bos.size(), duration ); 

    }

    public static void perf3() throws Exception {

        System.gc();

        ByteBuffer buff = ByteBuffer.allocate( 8192 );
        buff.mark();
        
        System.out.printf( "=============\n" );
        System.out.printf( "perf3\n" );

        long before = System.currentTimeMillis();
        
        //DefaultChunkWriter writer = new DefaultChunkWriter( out );

        int max = 2000000;

        byte[] key = new byte[8];
        byte[] value = new byte[16];

        int written = 0;

        VarintWriter varintWriter = new VarintWriter();
        
        for( int i = 0; i < max; ++i ) {

            // decide if we should flush now ...

            int width = key.length + value.length + 2;

            if ( buff.position() + width >= buff.limit() ) {

                written += buff.position();
                
                buff.array();
                buff.reset();

            }
            
            //out.write( (byte)0 );
            //buff.put( (byte)key.length );

            varintWriter.write( buff, key.length );
            buff.put( key );

            //buff.put( (byte)value.length );
            varintWriter.write( buff, value.length );
            buff.put( value );
            
        }

        //out.close();

        long after = System.currentTimeMillis();

        long duration = (after-before);
        
        System.out.printf( "Wrote %,d bytes in %,d ms\n", written, duration ); 

    }

    public static void perf4() throws Exception {

        System.gc();

        System.out.printf( "=============\n" );
        System.out.printf( "perf4\n" );
        
        FileOutputStream fos = new FileOutputStream( "/dev/null" ) ;
        BufferedOutputStream buff = new BufferedOutputStream( fos, 8192 );

        OutputStream out = fos;
        
        long before = System.currentTimeMillis();
        
        //DefaultChunkWriter writer = new DefaultChunkWriter( out );

        int max = 40000004;
        
        for( int i = 0; i < max; ++i ) {

            out.write( (byte)0 );

        }

        out.close();

        long after = System.currentTimeMillis();

        long duration = (after-before);
        
        System.out.printf( "Wrote %,d bytes in %,d ms\n", max, duration ); 

    }


    public static void perf6() throws Exception {

        System.gc();

        ByteArrayOutputStream out = new ByteArrayOutputStream( 100000000 ) ;

        ByteBuffer buff = ByteBuffer.allocate( 8192 );
        buff.mark();
        
        System.out.printf( "=============\n" );
        System.out.printf( "perf6\n" );

        long before = System.currentTimeMillis();
        
        //DefaultChunkWriter writer = new DefaultChunkWriter( out );

        int max = 2000000;

        byte[] key = new byte[8];
        byte[] value = new byte[16];

        int written = 0;

        VarintWriter varintWriter = new VarintWriter();
        
        for( int i = 0; i < max; ++i ) {

            // decide if we should flush now ...

            int width = key.length + value.length + 2;

            if ( buff.position() + width >= buff.limit() ) {

                written += buff.position();
                
                out.write( buff.array() );
                buff.reset();

            }
            
            //out.write( (byte)0 );
            //buff.put( (byte)key.length );

            varintWriter.write( buff, key.length );
            buff.put( key );

            //buff.put( (byte)value.length );
            varintWriter.write( buff, value.length );
            buff.put( value );
            
        }

        out.close();

        long after = System.currentTimeMillis();

        long duration = (after-before);
        
        System.out.printf( "Wrote %,d bytes in %,d ms\n", written, duration ); 

    }

    public static void main( String[] args ) throws Exception {

        perf1();
        perf5();
        perf6();
        perf4();
        perf3();
        perf2();
        
    }

}