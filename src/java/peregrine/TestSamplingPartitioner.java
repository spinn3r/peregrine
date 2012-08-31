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
    
    /**
     * The max number of buckets to consider before we punt.
     */
    public static final int LIMIT = 262144;

    // the MAX buckets we could have is 6M in a 100MB sampled chunk... so it
    // would be like 20-30MB of memory to build a partition for each sized
    // partition.

    public static List<Long> buildRandomGraph( int nr_nodes,
                                               int max_edges_per_node ) {

        Random r = new Random();

        Map<Long,Long> indegree = new HashMap();

        for( long i = 0; i < nr_nodes; ++i ) {
            indegree.put( i, 0L );
        }

        for( long i = 0; i < nr_nodes; ++i ) {

            HashSet<Long> linked = new HashSet();
            long source = i;
            
            //TODO: I think we should create a random number of edges.
            for( long j = 0; j < max_edges_per_node && j < i ; ++j ) {

                //connect this node to any of the existing other nodes other
                //than itself.
                long target = (long)(r.nextFloat() * (i - 1));

                if ( linked.contains( target ) ) {
                    //TODO: in the future we should just retry another random
                    //number say N times.
                    continue;
                }

                // addEdge( source, target )

                indegree.put( source, indegree.get( source ) + 1 );
                
                //System.out.printf( "%s -> %s\n", n_i, n_j );
                linked.add( target );
                
            }
            
        }

        List<Long> result = new ArrayList();

        for( long val : indegree.values() ) {
            result.add( padd( val ) );
        }
        
        return result;

    }

    public static List<Long> rangeDataSet( int max, int range, int offset ) {

        // TODO: create a artificial graph with a spike in ONE region ...
        
        Random r = new Random();
        
        List<Long> datapoints = new ArrayList();
        
        for( long i = 0; i < max; ++i ) {

            long val = padd( (long)(offset + r.nextInt( range ) ) );

            System.out.printf( "val: %s\n", val );
            
            datapoints.add( val ); 
        }

        return datapoints;
        
    }

    public static long padd( long val ) {

        val = val * 100000000;

        byte[] hash = Hashcode.getHashcode( Long.toString( val ) );
        byte[] zeros = new byte[] { 0, 0, 0, 0, 0 };
        System.arraycopy( zeros, 0, hash, 0, zeros.length );
        val = val + LongBytes.toLong( hash );

        return val;
        
    }

    public static void main( String[] args ) throws Exception {

        // System.out.printf( "%20s\n", padd( 0 ) );
        // System.out.printf( "%20s\n", padd( 1 ) );
        // System.out.printf( "%20s\n", padd( 2 ) );
        
        //List<Long> datapoints = rangeDataSet( 5000, 100, 10000 );
        List<Long> datapoints = buildRandomGraph( 10000, 100 );
        
        partition( datapoints, 10 );
        
    }

    public static Bucket partition( List<Long> data, int nr_partitions ) {

        // create the bucket index.
        
        Map<Long,Bucket> buckets = new HashMap();
        
        for ( int nr_buckets = nr_partitions; nr_buckets >= 0 && nr_buckets < Long.MAX_VALUE; nr_buckets = nr_buckets * 2 ) {

            long split = (Long.MAX_VALUE / nr_buckets);

            System.out.printf( "nr_buckets=%14s: split=%20s\n", nr_buckets, split );
            
            buckets.put( split, new Bucket( split ) );
            
        }

        //now route the values into buckets

        List<Long> splits = new ArrayList( buckets.keySet() );
        Collections.sort( splits );
        
        for( long value : data ) {

            for ( long split : splits ) {

                long bucket_id = value / split;

                System.out.printf( "bucket_id: %s\n", bucket_id );
                
                Bucket bucket = buckets.get( split );

                if ( bucket.size() >= LIMIT ) {
                    continue;
                }

                bucket.incr( bucket_id );

            }
            
        }

        //now move these into a partition which first matches.

        int idealPartitionSize = data.size() / nr_partitions;
        
        PartitionIndex partitionIndex = new PartitionIndex();

        for ( long split : splits ) {

            Bucket bucket = buckets.get( split );

            if ( bucket.size() >= LIMIT ) {
                continue;
            }

            RangeList rangeList = new RangeList( split, idealPartitionSize );

            List<Long> bucketKeys = new ArrayList( bucket.keySet() );
            Collections.sort( bucketKeys );

            Range current = new Range();

            for( long bucketKey : bucketKeys ) {

                int count = bucket.get( bucketKey );

                current.count += count;

                if ( current.count >= idealPartitionSize ) {

                    if ( current.end == -1 )
                        current.end = bucketKey;
                    
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

    public static void dump( List<RangeList> sortedRangeList ) {

        for ( RangeList rangeList : sortedRangeList ) {
            System.out.printf( "%25s: %s\n", rangeList.id, rangeList );
        }

    }

}

class Bucket extends HashMap<Long,Integer> {

    public long split;

    public Bucket( long split ) {
        this.split = split;
    }
    
    public void incr( long key ) {

        if ( containsKey( key ) ) {
            put( key, get( key ) + 1 );
        } else {
            put( key, 1 );
        }

    }
    
}

class Range {

    public long start = 0;
    public long end   = -1;

    public int count = 0;

    public boolean inclusive = false;
    
    public Range() { }

    public Range( long start ) {
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

    public long id;
    public int idealPartitionSize;
    
    public RangeList( long id, int idealPartitionSize ) {
        this.id = id;
        this.idealPartitionSize = idealPartitionSize;
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
            result += Math.abs( current.count - idealPartitionSize );
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

class PartitionIndex extends HashMap<Long,RangeList> { }