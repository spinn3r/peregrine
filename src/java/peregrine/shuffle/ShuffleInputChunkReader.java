package peregrine.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import peregrine.util.*;
import peregrine.config.*;
import org.jboss.netty.buffer.*;

import com.spinn3r.log5j.Logger;

/**
 * 
 */
public class ShuffleInputChunkReader {

    public static int QUEUE_CAPACITY = 100;

    private static PrefetchReaderManager prefetchReaderManager
        = new PrefetchReaderManager();

    private SimpleBlockingQueue<ShufflePacket> queue = null;

    private PrefetchReader prefetcher = null;
    
    private Partition partition;
    
    private String path;

    /**
     * The current shuffle packet.
     */
    private ShufflePacket pack = null;
    
    /**
     * Our current position in the key/value stream of items for this partition.
     */
    private Index partition_idx;;

    /**
     * The index of packet reads.
     */
    private Index packet_idx;
    
    private int key_offset;
    private int key_length;

    private int value_offset;
    private int value_length;

    VarintReader varintReader;

    /**
     * The header for this partition.
     */
    private ShuffleHeader header = null;
    
    public ShuffleInputChunkReader( Config config, Partition partition, String path )
        throws IOException {

        this.partition = partition;
        this.path = path;
        
        prefetcher = prefetchReaderManager.getInstance( config, path );

        // get the path that we should be working with.
        queue = prefetcher.lookup.get( partition );

        if ( queue == null )
            throw new IOException( "Queue is not defined for partition: " + partition );
        
        header = prefetcher.reader.getHeader( partition );

        if ( header == null ) {
            throw new IOException( "Unable to find header for partition: " + partition );
        }

        partition_idx  = new Index( header.count );
        packet_idx     = new Index( header.nr_packets );
        
    }

    public ShufflePacket getShufflePacket() {
        return pack;
    }

    public boolean hasNext() {

        boolean result = partition_idx.hasNext();

        if ( result == false )
            prefetcher.finished( partition );

        return result;

    }

    public void next() {

        while( true ) {

            if ( pack != null && pack.data.readerIndex() < pack.data.capacity() ) {

                this.key_length     = varintReader.read();
                this.key_offset     = pack.data.readerIndex();

                pack.data.readerIndex( pack.data.readerIndex() + key_length );
                
                this.value_length   = varintReader.read();
                this.value_offset   = pack.data.readerIndex();

                pack.data.readerIndex( pack.data.readerIndex() + value_length ); 

                partition_idx.next();
                
                return;
                
            } else if ( nextShufflePacket() ) {

                // we need to read the next ... 
                continue;

            } else {
                return;
            }

        }

    }

    private boolean nextShufflePacket() {
        
        if ( packet_idx.hasNext() ) {
            
            pack = queue.take();
            
            varintReader  = new VarintReader( pack.data );
            pack.data.readerIndex( 0 );

            packet_idx.next();
            
            return true;
            
        } else {
            return false;
        }

    }

    public ChannelBuffer getBuffer() {
        return prefetcher.reader.getBuffer();
    }

    public int keyOffset() {
        return key_offset;
    }

    public byte[] key() throws IOException {
        return readBytes( key_offset, key_length );
        
    }

    public byte[] value() throws IOException {
        return readBytes( value_offset, value_length );
    }

    private byte[] readBytes( int offset, int length ) throws IOException {

        byte[] data = new byte[ length ];
        pack.data.getBytes( offset, data );

        return data;
        
    }

    public int size() {
        return header.count;
    }

    @Override
    public String toString() {
        return String.format( "%s:%s:%s" , getClass().getName(), path, partition );
    }

    /**
     * Index for moving forward over items.
     */
    static class Index {

        private int idx = 0;
        private int max;
        
        public Index( int max ) {
            this.max = max;
        }
        
        public boolean hasNext() {
            return idx < max;
        }

        public void next() {
            ++idx;
        }

        public String toString() {
            return String.format( "idx: %s, max: %s", idx, max );
        }
        
    }
    
    static class PrefetchReader implements Callable {

        private static final Logger log = Logger.getLogger();

        private static ThreadFactory threadFactory = new DefaultThreadFactory( PrefetchReader.class );
        
        public Map<Partition,SimpleBlockingQueue<ShufflePacket>> lookup = new HashMap();

        private Map<Partition,SimpleBlockingQueue<Boolean>> finished = new ConcurrentHashMap();

        protected ShuffleInputReader reader = null;

        private String path;

        private PrefetchReaderManager manager = null;

        private Map<Partition,AtomicInteger> packetsReadPerPartition = new HashMap();

        protected ExecutorService executor =
            Executors.newCachedThreadPool( threadFactory );

        public PrefetchReader( PrefetchReaderManager manager, Config config, String path )
            throws IOException {

            this.manager = manager;
            this.path = path;

            // get the top priority replicas to reduce over.
            List<Replica> replicas = config.getMembership().getReplicasByPriority( config.getHost() );

            log.info( "Working with replicas %s for blocking queue on host %s", replicas, config.getHost() );
            
            List<Partition> partitions = new ArrayList();
            
            for( Replica replica : replicas ) {

                Partition part = replica.getPartition(); 
                
                lookup.put( part, new SimpleBlockingQueue( QUEUE_CAPACITY ) );
                finished.put( part, new SimpleBlockingQueue( 1 ) );
                
                packetsReadPerPartition.put( part, new AtomicInteger() );
                partitions.add( part );
                
            }
            
            // now open the shuffle file and read in the shuffle packets adding
            // them to the right queues.

            this.reader = new ShuffleInputReader( path, partitions );

        }

        /**
         * Called so that partitions that are read can note when they are finished.
         */
        public void finished( Partition partition ) {
            finished.get( partition ).put( Boolean.TRUE );
        }
        
        public Object call() throws Exception {

            try {

                log.info( "Reading from %s ...", path );

                int count = 0;

                while( reader.hasNext() ) {
                    
                    ShufflePacket pack = reader.next();

                    Partition part = new Partition( pack.to_partition ); 
                    
                    packetsReadPerPartition.get( part ).getAndIncrement();
                        
                    lookup.get( part ).put( pack );

                    ++count;
                    
                }

                // make sure all partitions are finished reading.
                for ( SimpleBlockingQueue _finished : finished.values() ) {
                    _finished.take();
                }

                log.info( "Reading from %s ...done (read %,d packets as %s)", path, count, packetsReadPerPartition );

                // remove thyself so that next time around there isn't a reference
                // to this path and a new reader will be created.
                manager.reset( path );

            } finally {
                reader.close();
            }
                
            return null;
            
        }
        
    }

    static class PrefetchReaderManager {

        private static final Logger log = Logger.getLogger();

        static Map<String,PrefetchReader> instances = new ConcurrentHashMap();

        public void reset( String path ) {

            synchronized( instances ) {

                // FIXME: right now this means that we startup 1 thread per
                // chunk which is not super efficient.
                PrefetchReader reader = instances.remove( path );
                reader.executor.shutdown();
                
            }
            
        }
        
        public PrefetchReader getInstance( Config config, String path )
            throws IOException {

            PrefetchReader result;

            result = instances.get( path );

            if ( result == null ) {

                // double check idiom.
                synchronized( instances ) {

                    result = instances.get( path );
                    
                    if ( result == null ) {

                        log.info( "Creating new prefetch reader for path: %s", path );
                        
                        result = new PrefetchReader( this, config, path );
                        instances.put( path, result );

                        result.executor.submit( result );

                    } 

                }
                
            } 

            return result;
            
        }

    }
    
}