package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

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

    //List<Tuple> tuples = new ArrayList();

    BulkArray tuples = new BulkArray();
    
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

        Tuple[] data = tuples.toArray();
        
        Arrays.sort( data );
        
    }
    
}
