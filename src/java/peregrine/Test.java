package peregrine;

import java.io.*;
import java.util.*;
import java.nio.*;
import java.nio.channels.*;

import peregrine.os.*;
import peregrine.util.*;
import peregrine.app.pagerank.*;

import com.sun.jna.Pointer;

import com.spinn3r.log5j.Logger;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.hadoop.*;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.utils.*;

// needed so that we can configure the InputFormat for Cassandra
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;

public class Test {

    private static final Logger log = Logger.getLogger();

    public static void main( String[] args ) throws Exception {

        ColumnFamilyInputFormat test = new ColumnFamilyInputFormat();

        Configuration conf = new Configuration();

        ConfigHelper.setInputColumnFamily( conf, "mykeyspace", "graph" );
        //ConfigHelper.setInputSlicePredicate( conf, new SlicePredicate() );
        ConfigHelper.setInitialAddress( conf, "localhost" );
        ConfigHelper.setRpcPort( conf, "9160" );

        ConfigHelper.setPartitioner(conf, "org.apache.cassandra.dht.RandomPartitioner" );

        //ConfigHelper.setInputRange( conf, "0", "18446744073709551616" );

        SlicePredicate sp = new SlicePredicate();

        SliceRange sr = new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 100 );
        sp.setSlice_range(sr);

        ConfigHelper.setInputSlicePredicate(conf, sp);

        //System.out.printf( "FIXME: %s\n", ConfigHelper.keyRangeToString( new KeyRange() ) );

        JobContext jobContext = new JobContext( conf, new JobID() );
        
        test.getSplits( jobContext );
        
        //ColumnFamilyInputFormat inputFormat = new ColumnFamilyInputFormat();

        /*
        
        Runtime r = Runtime.getRuntime();

        System.gc();
        long before = r.totalMemory() - r.freeMemory();

        int max = 10000;
        
        List list = new ArrayList( max );

        for( int i = 0; i < max; ++i ) {
            list.add( new Object() );
        }

        System.gc();
        long after = r.totalMemory() - r.freeMemory();

        long used = after - before;
        
        System.out.printf( "used: %,d bytes\n", used );

        System.out.printf( "size: %,d\n", list.size() );

        */
        
        // EfficientTreeMap<byte[],byte[]> map = new EfficientTreeMap();

        // Runtime r = Runtime.getRuntime();

        // System.gc();
        // long before = r.totalMemory() - r.freeMemory();

        // for( int i = 0; i < 10000; ++i ) {

        //     byte[] key = Hashcode.getHashcode( "" + i );
        //     map.put( key , key );
            
        // }

        // System.gc();
        // long after = r.totalMemory() - r.freeMemory();

        // long used = after - before;
        
        // System.out.printf( "used: %,d bytes\n", used );

        // System.out.printf( "size: %,d\n", map.size() );

        // 80 bytes per entry... 
        
        // // mmap a file

        // File file = new File( args[0] );
        
        // FileInputStream in = new FileInputStream( file );
        // FileChannel channel = in.getChannel();
        // int fd = Native.getFd( in.getFD() );

        // long offset = 0;
        // long length = file.length();
        
        // MappedByteBuffer map = channel.map( FileChannel.MapMode.READ_ONLY,
        //                                     offset,
        //                                     length );

        // // 56 per ms... 
        
        // int max = 100000;

        // Pointer ptr = mman.mmap( length, mman.PROT_READ, mman.MAP_SHARED | mman.MAP_LOCKED, fd, offset );

        // long before = System.currentTimeMillis();
        
        // for ( int i = 0; i < max; ++i ) {

        //     mman.mlock( ptr, length );
        //     mman.munlock( ptr, length );
            
        // }

        // long after = System.currentTimeMillis();

        // System.out.printf( "duration: %,d ms\n", (after-before) );
        
        // // mlock the region
        
    }
    
}

class EfficientTreeMap<K,V> extends TreeMap implements Map {

    public EfficientTreeMap() {
        super( new ByteArrayComparator() ) ;
    }

}

class ByteArrayComparator implements Comparator {

    public int compare( Object o1, Object o2 ) {

        byte[] v1 = (byte[]) o1;
        byte[] v2 = (byte[]) o2;

        int len1 = v1.length;
        int len2 = v2.length;
        int n = Math.min(len1, len2);

        int i = 0;
        int j = 0;

        int k = i;
        final int lim = n + i;
        while (k < lim) {
            byte c1 = v1[k];
            byte c2 = v2[k];
            if (c1 != c2) {
                return c1 - c2;
            }
            k++;
        }
        
        return len1 - len2;
    }

}

