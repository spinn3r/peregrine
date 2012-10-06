package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.nio.*;
import java.nio.channels.*;
import java.lang.reflect.*;
import java.math.*;

import peregrine.os.*;
import peregrine.util.*;
import peregrine.util.primitive.*;
import peregrine.app.pagerank.*;
import peregrine.config.*;
import peregrine.worker.*;
import peregrine.rpc.*;
import peregrine.sort.*;
import peregrine.controller.*;

import org.jboss.netty.buffer.*;

import com.sun.jna.Pointer;

import com.spinn3r.log5j.Logger;

import java.nio.charset.Charset;

public class Test {

    private static final Logger log = Logger.getLogger();

    public static StructReader mean( List<StructReader> values ) {

        byte[] result = new byte[ values.get( 0 ).length() ];

        for( int i = 0; i < result.length; ++i ) {

            int sum = 0;

            for( StructReader current : values ) {
                sum += (current.getByte( i ) & 0xFF);
                //sum += current.getByte( i ) ;
            }

            result[i] = (byte)(sum / values.size());
            sum = 0;
            
        }

        return StructReaders.wrap( result );

    }

    public static long meanLongs( List<Long> values ) {

        long sum = 0;

        for ( long value : values ) {
            sum += value;
        }

        return (long) sum / values.size();

    }

    public static void main( String[] args ) throws Exception {

        BigInteger bi = new BigInteger( LongBytes.toByteArray( Long.MAX_VALUE ) );

        System.out.printf( "bi: %s\n", bi );
        
        // Set<StructReader> set = new HashSet();

        // set.add( StructReaders.hashcode( "asdf" ) );
        
        // System.out.printf( "%s\n", set.contains( StructReaders.hashcode( "asdf" ) ) );
        
        // ChannelBuffer buff = ChannelBuffers.wrappedBuffer( new byte[128] );

        // buff.writerIndex( 0 );
        
        // VarintWriter.write( buff, Integer.MAX_VALUE );

        // long before = System.currentTimeMillis();

        // int max = 10000000;
        
        // for( int i = 0; i < max; ++i ) {
        //     buff.readerIndex( 0 );
        //     int result = VarintReader.read( buff );
        // }

        // long after = System.currentTimeMillis();

        // System.out.printf( "duration: %,d\n", (after-before) );
        
        //System.out.printf( "%s\n", NonceFactory.newNonce() );
        
        // List<StructReader> list = new ArrayList();
        // List<Long> longs = new ArrayList();
        
        // for ( long value = 0; value < 1024; ++value ) {
        //     list.add( StructReaders.wrap( value ) );
        //     longs.add( value );
        // }

        // StructReader mean = mean( list );

        // System.out.printf( "%s\n", mean.readLong() );
        // System.out.printf( "%s\n", meanLongs( longs ) );
        
        //System.out.printf( "%s\n", JobState.SUBMITTED );
        //System.out.printf( "%s\n", JobState.SUBMITTED.getClass().newInstance() );

        // Job job = new Job();

        // Batch batch = new Batch();
        // batch.add( job );

        // Batch ser = new Batch();
        // ser.fromMessage( batch.toMessage() );

        // System.out.printf( "%s\n", ser.toString() );

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

}

class Foo extends BaseFoo<Foo> {

    public Foo() {
        init( this );
    }
    
}

class  BaseFoo<T> {

    protected T instance;
    
    public void init( T instance ) {
        this.instance = instance;
    }

    public T getFoo() {
        return instance;
                            
    }
    
}