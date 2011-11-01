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

    public ParallelShuffleInputChunkReader( Config config, Partition partition, String path ) {

        this.config = config;
        this.partition = partition;
        this.path = path;

        initWhenRequired();

        // get the path that we should be working with.
        queue = prefetcher.lookup.get( partition );
        
    }

    public ShufflePacket nextShufflePacket() {
        return queue.take();
    }


    public boolean hasNext() {
        return queue.size() > 0;
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
        
        public PrefetchReader( ParallelShuffleInputChunkReader parent ) {
            this.parent = parent;
        }
        
        public Object call() throws Exception {

            // now open the shuffle file and read in the shuffle packets adding
            // them to the right queues.

            return null;
            
        }
        
    }
    
}