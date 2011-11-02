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
public class ShuffleInputChunkReader {

    public static int QUEUE_CAPACITY = 100;

    private static final Logger log = Logger.getLogger();

    private static PrefetchReaderManager prefetchReaderManager
        = new PrefetchReaderManager();

    private SimpleBlockingQueue<ShufflePacket> queue = null;

    private PrefetchReader prefetcher = null;
    
    private Config config;

    private Partition partition;
    
    private String path;

    /**
     * The current shuffle packet.
     */
    private ShufflePacket pack = null;
    
    /**
     * Our current position in the key/value stream of items.
     */
    private int idx = 0;

    /**
     * The index of packet reads.
     */
    private int packet_idx = 0;
    
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

        this.config = config;
        this.partition = partition;
        this.path = path;
        
        prefetcher = prefetchReaderManager.getInstance( config, path );

        // get the path that we should be working with.
        queue = prefetcher.lookup.get( partition );

        header = prefetcher.reader.getHeader( partition );

        if ( header == null ) {
            throw new IOException( "Unable to find header for partition: " + partition );
        }
        
    }

    public ShufflePacket getShufflePacket() {
        return pack;
    }

    public boolean hasNext() {
        return idx < header.count;
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

                ++idx;
                
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
        
        if ( packet_idx < header.nr_packets ) {
            
            pack = queue.take();
            
            varintReader  = new VarintReader( pack.data );
            pack.data.readerIndex( 0 );

            ++packet_idx;
            
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

    static class PrefetchReader implements Callable {

        private static final Logger log = Logger.getLogger();

        public Map<Partition,SimpleBlockingQueue<ShufflePacket>> lookup = new HashMap();

        protected ShuffleInputReader reader = null;

        private Config config;
        
        private String path;

        private PrefetchReaderManager manager = null;
        
        public PrefetchReader( PrefetchReaderManager manager, Config config, String path )
            throws IOException {

            this.manager = manager;
            this.config = config;
            this.path = path;

            // get the top priority replicas to reduce over.
            List<Replica> replicas = config.getMembership().getReplicasByPriority( config.getHost() );

            log.info( "Working with replicas: %s", replicas );
            
            List<Partition> partitions = new ArrayList();
            
            for( Replica replica : replicas ) {

                lookup.put( replica.getPartition(), new SimpleBlockingQueue( QUEUE_CAPACITY ) );

                partitions.add( replica.getPartition() );
                
            }
            
            // now open the shuffle file and read in the shuffle packets adding
            // them to the right queues.

            this.reader = new ShuffleInputReader( path, partitions );

        }
        
        public Object call() throws Exception {

            log.info( "Reading from %s ...", path );

            int count = 0;
            
            while( reader.hasNext() ) {
                
                ShufflePacket pack = reader.next();
                lookup.get( new Partition( pack.to_partition ) ).put( pack );

                ++count;
                
            }
            
            log.info( "Reading from %s ...done (read %,d packets)", path, count );

            // remove thyself
            manager.reset( path );
            
            return null;
            
        }
        
    }

    static class PrefetchReaderManager {

        private static final Logger log = Logger.getLogger();

        private static ExecutorService executors =
            Executors.newCachedThreadPool( new DefaultThreadFactory( PrefetchReaderManager.class) );

        static Map<String,PrefetchReader> instances = new ConcurrentHashMap();

        public void reset( String path ) {
            instances.remove( path );
        }
        
        public PrefetchReader getInstance( Config config, String path )
            throws IOException {

            PrefetchReader result = instances.get( path );

            if ( result == null ) {

                // double check idiom.
                synchronized( instances ) {

                    result = instances.get( path );
                    
                    if ( result == null ) {

                        log.info( "Creating new prefetch reader for path: %s", path );
                        
                        result = new PrefetchReader( this, config, path );
                        instances.put( path, result );

                        executors.submit( result );

                    } 

                }
                
            }

            return result;
            
        }

    }
    
}