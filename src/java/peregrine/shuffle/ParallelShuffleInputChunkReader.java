package peregrine.shuffle;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.values.*;
import peregrine.io.async.*;
import peregrine.io.chunk.*;

import org.jboss.netty.buffer.*;

import com.spinn3r.log5j.Logger;

/**
 * 
 */
public class ParallelShuffleInputChunkReader {

    public static int QUEUE_CAPACITY = 100;

    private static PrefetchReader prefetcher = null;

    private SimpleBlockingQueue<ShufflePacket> queue = null;

    private Config config;

    private Partition partition;
    
    private String path;

    private int count;
    
    public ParallelShuffleInputChunkReader( Config config, Partition partition, String path ) {

        this.config = config;
        this.partition = partition;
        this.path = path;

        initWhenRequired();

        // get the path that we should be working with.
        queue = prefetcher.lookup.get( partition );

        count = prefetcher.reader.getHeader( partition ).count;
        
    }

    public ShufflePacket nextShufflePacket() {
        return queue.take();
    }

    public boolean hasNext() {
        return queue.size() > 0;
    }
    
    public int size() {
        return count;
    }
    
    private boolean initRequired() {
        return prefetcher == null;
    }

    private void initWhenRequired() {
        
        if( initRequired() ) {

            synchronized( this ) {

                // double check idiom
                if( initRequired() ) {

                    prefetcher = new PrefetchReader( this );

                    // read all of the partitions this host is assigned.
                    
                    List<Partition> partitions =
                        config.getMembership().getPartitions( config.getHost() );

                    for( Partition part : partitions ) {
                        prefetcher.lookup.put( part, new SimpleBlockingQueue( QUEUE_CAPACITY ) );
                    }

                } 

            }
            
        }

    }

    static class PrefetchReader implements Callable {

        public Map<Partition,SimpleBlockingQueue<ShufflePacket>> lookup = new HashMap();

        private ParallelShuffleInputChunkReader parent;

        protected ShuffleInputReader2 reader = null;
        
        public PrefetchReader( ParallelShuffleInputChunkReader parent ) {
            this.parent = parent;
        }
        
        public Object call() throws Exception {

            Config config = parent.config;

            // get the top priority replicas to reduce over.
            List<Replica> replicas = config.getMembership().getReplicasByPriority( config.getHost() );

            List<Partition> partitions = new ArrayList();

            for( Replica replica : replicas ) {
                partitions.add( replica.getPartition() );
            }
            
            // now open the shuffle file and read in the shuffle packets adding
            // them to the right queues.

            this.reader = new ShuffleInputReader2( parent.path, partitions );

            while( reader.hasNext() ) {

                ShufflePacket pack = reader.next();
                lookup.get( new Partition( pack.to_partition ) ).put( pack );
                
            }
            
            return null;
            
        }
        
    }
    
}