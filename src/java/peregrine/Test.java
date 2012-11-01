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

    static class JobSortComparator extends StrictSortComparator {

        @Override
        public StructReader getSortKey( StructReader key, StructReader value ) {
            // read the first 8 bytes which is the long representation
            // of what we should sort be sorting by.
            return StructReaders.join( value.slice( 0, 8 ), key.slice() );
        }

    }

    public static void sync( List<MappedFileWriter> writers ) throws IOException {

        if ( writers.size() == 0 )
            return;

        System.out.printf( "Syncing %,d writers.\n", writers.size() );
        
        for( MappedFileWriter writer : writers ) {
            writer.sync();
            writer.close();
        }
        
    }
    
    public static void main( String[] args ) throws Exception {

        //Getopt getopt = new Getopt( args );

        //System.out.printf( "extract: %s\n", getopt.getBoolean( "extract" ) );

        System.out.printf( "%s\n" , Longs.format( 12345 ) );
        System.out.printf( "%s\n" , Longs.format( 1234567890 ) );
        System.out.printf( "%s\n" , Longs.format( 123456789000L ) );
        System.out.printf( "%s\n" , Longs.format( 1234567890000L ) );
        
        //Thread.sleep( 10000L );

        // int size = 16000000;
        // int nr_files = 200;

        // if ( args.length == 2 ) {
        //     size = Integer.parseInt( args[0] );
        //     nr_files = Integer.parseInt( args[1] );
        // }
        
        // System.out.printf( "size: %,d\n", size );
        // System.out.printf( "nr_files: %,d\n", nr_files );

        // byte[] data = new byte[ size ];

        // ChannelBuffer buff = ChannelBuffers.wrappedBuffer( data );

        // for( int i = 0; i < nr_files; ++i ) {
            
        //     String path = String.format( "/d0/test-%s.dat", i );

        //     File file = new File( path );

        //     if ( file.exists() == false && file.createNewFile() == false )
        //         throw new IOException();

        //     MappedFileWriter writer = new MappedFileWriter( null, file );

        //     fcntl.posix_fallocate( writer.getFd(), 0, size );
        //     writer.close();

        // }

        // int sync_interval = 250000000;

        // int sync_pending = 0;

        // List<MappedFileWriter> pending = new ArrayList();
        
        // //FIXME: only sync every 100MB or so?
        // for( int i = 0; i < nr_files; ++i ) {

        //     String path = String.format( "/d0/test-%s.dat", i );

        //     System.out.printf( "%s\n", path );
            
        //     MappedFileWriter writer = new MappedFileWriter( null, path );
        //     writer.write( buff );

        //     if ( sync_pending >= sync_interval ) {
        //         sync( pending );
        //         sync_pending = 0;
        //         pending = new ArrayList();
        //     } else {

        //         pending.add( writer );
        //         sync_pending += size;
                
        //     }

        // }

        // sync( pending );

        //System.out.printf( "%s\n", Longs.format( 1000 ) );
        //System.out.printf( "%s\n", Longs.format( 1100000 ) );
        // System.out.printf( "%s\n", Longs.format( 999000 ) );
        // System.out.printf( "%s\n", Longs.format( 999999 ) );
        // System.out.printf( "%s\n", Longs.format( 1001 ) );

        //Default

        // new Initializer().logger( "test" );

        // List<StructReader> list = new ArrayList();

        // /*
        // list.add( new StructReader( new byte[] { (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0xfc, (byte)0x4e, (byte)0x10, (byte)0x2b, (byte)0x3d, (byte)0x9b, (byte)0x73, (byte)0xcb } ) );
        // list.add( new StructReader( new byte[] { (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x05, (byte)0x24, (byte)0xdf, (byte)0x25, (byte)0x5a, (byte)0x48, (byte)0x25, (byte)0x4e } ) );
        // list.add( new StructReader( new byte[] { (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x0d, (byte)0xb4, (byte)0x7f, (byte)0x39, (byte)0x52, (byte)0x8b, (byte)0xa1, (byte)0x95 } ) );
        // */

        // list.add( new StructReader( new byte[] { (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x01, (byte)0xc3, (byte)0xf5, (byte)0xf1, (byte)0x70, (byte)0x34, (byte)0x34, (byte)0x2c, (byte)0x2e } ) );
        // list.add( new StructReader( new byte[] { (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x05, (byte)0x24, (byte)0xdf, (byte)0x25, (byte)0x5a, (byte)0x48, (byte)0x25, (byte)0x4e } ) );

        // ComputePartitionTableJob.dump( "FIXME: before: ", list );

        // //Collections.sort( list, new JobSortComparator() );
        // Collections.sort( list, new StrictStructReaderComparator() );
        
        // ComputePartitionTableJob.dump( "FIXME: after: ", list );
        
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