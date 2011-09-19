package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;

public class ShuffleManager {

    public static Map<Integer,PartitionShuffleBuffer> bufferMap = new ConcurrentHashMap();
    
    public static void accept( int partition,
                               byte[] key,
                               byte[] value ) {

        PartitionShuffleBuffer buffer = getBuffer( partition );

        buffer.accept( key, value );

    }

    private static PartitionShuffleBuffer getBuffer( int partition ) {

        PartitionShuffleBuffer buffer = bufferMap.get( partition );

        if ( buffer == null ) {
            buffer = new PartitionShuffleBuffer( partition );
            bufferMap.put( partition, buffer );
        }
        
        return buffer;
        
    }
    
    public static void cleanup( Partition partition ) {

        PartitionShuffleBuffer buffer = getBuffer( partition.getId() );
        buffer.cleanup();
        
    }

}

class PartitionShuffleBuffer {

    List<Tuple> tuples = new ArrayList();

    private int partition = 0;

    public PartitionShuffleBuffer( int partition ) {
        this.partition = partition;
    }
    
    public void accept( byte[] key,
                        byte[] value ) {

        tuples.add( new Tuple( key, value ) );
        
    }

    public void cleanup() {

        System.out.printf( "shuffle buffer cleanup of %,d entries from partition %,d.\n", tuples.size(), partition );
        
        Collections.sort( tuples );
    }
    
}

class Tuple implements Comparable {

    protected byte[] key = null;
    protected byte[] value = null;

    private long keycmp = -1;

    public Tuple( byte[] key, byte[] value ) {
        keycmp = Hashcode.toLong( key );
    }

    public int compareTo( Object o ) {

        long result = keycmp - ((Tuple)o).keycmp;

        // TODO: is there a faster way to do this?
        if ( result < 0 )
            return -1;
        else if ( result > 0 )
            return 1;

        return 0;
        
    }
    
}