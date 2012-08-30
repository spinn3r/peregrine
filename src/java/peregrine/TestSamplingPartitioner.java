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

import org.jboss.netty.buffer.*;

import com.sun.jna.Pointer;

import com.spinn3r.log5j.Logger;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.hadoop.*;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.db.*;

import java.nio.charset.Charset;

// needed so that we can configure the InputFormat for Cassandra
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;

public class TestSamplingPartitioner {
    
    private static final Logger log = Logger.getLogger();
    
    // the max number of buckets to consider before we punt.
    public static final int LIMIT = 65536;
    
    public static void main( String[] args ) throws Exception {

        int max   = 20000;
        int range = 50000;
        int offset = 1000;
        
        Random r = new Random();
        
        List<Integer> data = new ArrayList();
        
        for( int i = 0; i < max; ++i ) {
            data.add( offset + r.nextInt( range ) );            
        }

        partition( data, 10 );
        
    }

    public static Bucket partition( List<Integer> data, int nr_partitions ) {

        // create the bucket index.
        
        Map<Integer,Bucket> buckets = new HashMap();
        
        for ( int i = nr_partitions; i >= 0 && i < Integer.MAX_VALUE; i = i * 2 ) {

            int split = (int)(Integer.MAX_VALUE / i);

            System.out.printf( "%10s: %20s\n", i, split );
            
            buckets.put( split, new Bucket() );
        }

        //now route the values into buckets

        List<Integer> splits = new ArrayList( buckets.keySet() );
        Collections.sort( splits );
        
        for( int value : data ) {

            for ( int split : splits ) {

                int bucket_id = value / split;

                Bucket bucket = buckets.get( split );

                if ( bucket.size() >= LIMIT ) {
                    continue;
                }

                bucket.incr( bucket_id );

            }
            
        }

        //now move these into a partition which first matches.

        int idealValuesPerPartition = data.size() / nr_partitions;
        
        PartitionIndex partitionIndex = new PartitionIndex();

        for ( int split : splits ) {

            Bucket bucket = buckets.get( split );

            if ( bucket.size() >= LIMIT ) {
                continue;
            }

            RangeList rangeList = new RangeList( split, idealValuesPerPartition );

            List<Integer> bucketKeys = new ArrayList( bucket.keySet() );
            Collections.sort( bucketKeys );

            Range current = new Range();

            for( int bucketKey : bucketKeys ) {

                int count = bucket.get( bucketKey );

                current.count += count;

                if ( current.count >= idealValuesPerPartition ) {
                    rangeList.add( current );
                    current = new Range( current.end );
                }

                current.end = bucketKey;

            }

            if ( ! rangeList.last().equals( current ) ) {
                rangeList.add( current );
            }

            rangeList.last().inclusive = true;
            
            partitionIndex.put( split , rangeList );

        }

        List<RangeList> sortedRangeList = new ArrayList( partitionIndex.values() );
        Collections.sort( sortedRangeList );

        dump( sortedRangeList );

        return null;
        
    }

    public static void dump( Map<Integer,Bucket> buckets ) {

        List<Integer> keys = new ArrayList( buckets.keySet() );

        Collections.sort( keys );
        
        for ( int key : keys ) {
            System.out.printf( "%10s: %s\n", key, buckets.get( key ) );
        }
        
    }

    public static void dump( PartitionIndex partitionIndex ) {

        List<Integer> keys = new ArrayList( partitionIndex.keySet() );
        Collections.sort( keys );
        
        for ( int key : keys ) {
            System.out.printf( "%10s: %s\n", key, partitionIndex.get( key ) );
        }

    }

    public static void dump( List<RangeList> sortedRangeList ) {

        for ( RangeList rangeList : sortedRangeList ) {
            System.out.printf( "%10s: %s\n", rangeList.id, rangeList );
        }

    }

}

class Bucket extends HashMap<Integer,Integer> {

    public void incr( int key ) {

        if ( containsKey( key ) ) {
            put( key, get( key ) + 1 );
        } else {
            put( key, 1 );
        }

    }
    
}

class Range {

    public int start = 0;
    public int end   = -1;

    public int count = 0;

    public boolean inclusive = false;
    
    public Range() { }

    public Range( int start ) {
        this.start = start;
    }

    public String toString() {

        if ( inclusive ) {
            return String.format( "[%s,%s]=%s", start, end, count );
        } else { 
            return String.format( "[%s,%s)=%s", start, end, count );
        }
    }

    public boolean equals( Range range ) {
        return start == range.start &&
               end == range.end;
    }
    
}

class RangeList extends ArrayList<Range> implements Comparable<RangeList> {

    public int id;
    public int idealValuesPerPartition;
    
    public RangeList( int id, int idealValuesPerPartition ) {
        this.id = id;
        this.idealValuesPerPartition = idealValuesPerPartition;
    }
    
    public String toString() {
        return String.format( "size=%s, distance=%s: %s", size(), distance(), super.toString() );
    }

    /**
     * Compute the distance of this RangeList from the ideal distribution.
     */
    public int distance() {

        int result = 0;

        for( Range current : this ) {
            result += Math.abs( current.count - idealValuesPerPartition );
        }

        return result;
        
    }

    @Override
    public int compareTo(RangeList o) {
        return distance() - o.distance();
    }
    
    public Range last() {
        return get( size() - 1 );
    }

}

class PartitionIndex extends HashMap<Integer,RangeList> { }