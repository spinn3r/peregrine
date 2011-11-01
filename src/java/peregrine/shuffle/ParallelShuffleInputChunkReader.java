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
public class ParallelShuffleInputChunkReader implements ShuffleInputChunkReader {

    public static int QUEUE_CAPACITY = 100;

    private static final Logger log = Logger.getLogger();

    private static ExecutorService executors =
        Executors.newCachedThreadPool( new DefaultThreadFactory( ParallelShuffleInputChunkReader.class) );

    private static PrefetchReader prefetcher = null;

    private SimpleBlockingQueue<ShufflePacket> queue = null;

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
    
    public ParallelShuffleInputChunkReader( Config config, Partition partition, String path )
        throws IOException {

        this.config = config;
        this.partition = partition;
        this.path = path;

        initWhenRequired();

        // get the path that we should be working with.
        queue = prefetcher.lookup.get( partition );

        header = prefetcher.reader.getHeader( partition );

    }

    @Override
    public ShufflePacket getShufflePacket() {
        return pack;
    }

    @Override
    public boolean hasNext() {
        return idx < header.count;
    }

    @Override
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

    @Override
    public ChannelBuffer getBuffer() {
        return prefetcher.reader.getBuffer();
    }

    @Override
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

    @Override
    public int size() {
        return header.count;
    }

    @Override
    public String toString() {
        return String.format( "%s:%s:%s" , getClass().getName(), path, partition );
    }

    private boolean initRequired() {
        return prefetcher == null;
    }

    private void initWhenRequired() throws IOException {
        
        if( initRequired() ) {

            synchronized( this ) {

                // double check idiom
                if( initRequired() ) {

                    prefetcher = new PrefetchReader( this );

                    // read all of the partitions this host is assigned.
                    
                    List<Partition> partitions =
                        config.getMembership().getPartitions( config.getHost() );

                    if ( partitions == null )
                        throw new RuntimeException( String.format( "No partitions defined for host: %s" , config.getHost() ) );
                        
                    for( Partition part : partitions ) {
                        prefetcher.lookup.put( part, new SimpleBlockingQueue( QUEUE_CAPACITY ) );
                    }

                    executors.submit( prefetcher );
                    
                } 

            }
            
        }

    }

    static class PrefetchReader implements Callable {

        private static final Logger log = Logger.getLogger();

        public Map<Partition,SimpleBlockingQueue<ShufflePacket>> lookup = new HashMap();

        private ParallelShuffleInputChunkReader parent;

        protected ShuffleInputReader2 reader = null;
        
        public PrefetchReader( ParallelShuffleInputChunkReader parent )
            throws IOException {

            this.parent = parent;

            Config config = parent.config;

            // get the top priority replicas to reduce over.
            List<Replica> replicas = config.getMembership().getReplicasByPriority( config.getHost() );

            log.info( "Working with replicas: %s", replicas );
            
            List<Partition> partitions = new ArrayList();

            for( Replica replica : replicas ) {
                partitions.add( replica.getPartition() );
            }
            
            // now open the shuffle file and read in the shuffle packets adding
            // them to the right queues.

            this.reader = new ShuffleInputReader2( parent.path, partitions );

        }
        
        public Object call() throws Exception {

            while( reader.hasNext() ) {

                System.out.printf( "FIXME found one\n" );
                
                ShufflePacket pack = reader.next();
                lookup.get( new Partition( pack.to_partition ) ).put( pack );
                
            }

            System.out.printf( "FIXME: done\n" );
            
            return null;
            
        }
        
    }
    
}