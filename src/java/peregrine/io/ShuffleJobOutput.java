package peregrine.io;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.net.*;
import java.util.concurrent.*;

import org.jboss.netty.buffer.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.keys.*;
import peregrine.map.*;
import peregrine.shuffle.*;
import peregrine.util.*;
import peregrine.values.*;
import peregrine.io.chunk.*;
import peregrine.io.async.*;
import peregrine.pfs.*;
import peregrine.pfsd.shuffler.*;

import com.spinn3r.log5j.Logger;

import static peregrine.pfsd.FSPipelineFactory.*;

public class ShuffleJobOutput implements JobOutput, LocalPartitionReaderListener {

    private static final Logger log = Logger.getLogger();

    private static ExecutorService executors =
        Executors.newFixedThreadPool( 1, new DefaultThreadFactory( ShuffleJobOutput.class) );

    /**
     * Write in 2MB chunks at ~100MB output this is only 2MB extra memory
     * potentially wasted which would be at most 2%.
     */
    public static final int EXTENT_SIZE = 2097152;
    
    private int partitions = 0;

    protected ChunkReference chunkRef = null;

    protected peregrine.pfsd.shuffler.Shuffler shuffler = null;

    protected Membership partitionMembership;

    protected ShuffleOutput shuffleOutput;

    protected String name;

    protected Future future = null;

    protected Config config;
    
    public ShuffleJobOutput( Config config ) {
        this( config, "default" );
    }
        
    public ShuffleJobOutput( Config config, String name ) {

        this.config = config;
        this.name = name;
        
        this.partitionMembership = config.getPartitionMembership();

        this.partitions = partitionMembership.size();

        // we need to buffer the output so that we can route it to the right
        // host.
        
    }
    
    @Override
    public void emit( byte[] key , byte[] value ) {

        Partition target = Config.route( key, partitions, true );

        int from_partition  = chunkRef.partition.getId();
        int from_chunk      = chunkRef.local;

        int to_partition    = target.getId();

        shuffleOutput.write( to_partition, key, value );
        
    }

    @Override 
    public void close() throws IOException {

        try {
            
            if ( future != null ) 
                future.get();
            
        } catch ( Exception e ) {
            throw new IOException( e );
        }
        
    }

    @Override 
    public void onChunk( ChunkReference chunkRef ) {

        this.chunkRef = chunkRef;

        this.shuffleOutput = new ShuffleOutput( chunkRef, name );
        
    }

    @Override 
    public void onChunkEnd( ChunkReference ref ) {
        
        try {
        
            if ( future != null )
                future.get();

            future = executors.submit( new ShuffleFlushCallable( config, shuffleOutput ) );

        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }

    }
    
}

class ShuffleFlushCallable implements Callable {

    private static final Logger log = Logger.getLogger();

    private ShuffleOutput output = null;

    private Config config = null;
    
    public ShuffleFlushCallable( Config config, ShuffleOutput output ) {
        this.config = config;
        this.output = output;
    }
    
    public Object call() throws Exception {

        // FIXME: dont allow us to close somethin out TWICE

        if ( output.flushing )
            return null;

        output.flushing = true;
        
        log.info( "Closing shuffle job output for chunk: %s", output.chunkRef );

        Map<Integer,RemoteChunkWriterClient> partitionOutput = getPartitionOutput();

        // now read the data and write it to all clients .. 

        int count = 0;

        // FIXME: ANY of these writes can fail and if they do we need to
        // continue and just gossip that they have failed...  this includes
        // write() AND close()

        for( ShuffleOutputExtent extent : output.extents ) {

            ChannelBuffer buff = extent.buff;

            for ( int i = 0; i < extent.count; ++i ) {

                int to_partition = buff.readInt();
                int length       = buff.readInt();

                ChannelBuffer slice = buff.slice( buff.readerIndex() , length );

                RemoteChunkWriterClient client = partitionOutput.get( to_partition );

                if ( client == null )
                    throw new Exception( "NO client for partition: " + to_partition );
                
                client.write( slice );

                // bump up the writer index now for the next reader.
                buff.readerIndex( buff.readerIndex() + length );
                
                ++count;
                
            }

        }
        
        // now close all clients and we are done.
        
        for( RemoteChunkWriterClient client : partitionOutput.values() ) {
            client.close();
        }

        log.info( "Shuffled %,d entries.", count );

        return null;
        
    }

    private Map<Integer,RemoteChunkWriterClient> getPartitionOutput() {

        try {

            Map<Integer,RemoteChunkWriterClient> clients = new HashMap();

            Membership membership = config.getPartitionMembership();
            
            Set<Partition> partitions = membership.getPartitions();
            
            for( Partition part : partitions ) {

                List<Host> hosts = membership.getHosts( part );
                
                String path = String.format( "/shuffle/%s/from-partition/%s/from-chunk/%s/to-partition/%s",
                                             output.name,
                                             output.chunkRef.partition.getId(),
                                             output.chunkRef.local,
                                             part.getId() );

                RemoteChunkWriterClient client = new RemoteChunkWriterClient( hosts, path );

                clients.put( part.getId(), client );
                
            }

            return clients;
            
        } catch ( Exception e ) {
            // This should be ok as it will cause the map job to fail which will
            // then be caught by gossip.
            throw new RuntimeException( e );
        }

    }
    
}

/**
 * 
 */
class ShuffleOutput {

    private static final Logger log = Logger.getLogger();

    List<ShuffleOutputExtent> extents = new ArrayList();

    ShuffleOutputExtent extent = null;

    protected int partitions;

    protected ChunkReference chunkRef = null;

    protected String name = null;

    protected boolean flushing = false;
    
    public ShuffleOutput( ChunkReference chunkRef, String name ) {

        this.chunkRef = chunkRef;
        this.name = name;
        
        rollover();

    }
    
    public void write( int to_partition, byte[] key, byte[] value ) {

        // the max width that this write could consume.  2 ints for the
        // partition and the width of the value and then the length of the key
        // and the lenght of the value + two ints for the varints.

        int key_value_length =
            VarintWriter.sizeof( key.length ) +
            key.length +
            VarintWriter.sizeof( value.length ) +
            value.length
            ;
        
        int write_width =
            IntBytes.LENGTH +
            IntBytes.LENGTH +
            key_value_length
            ;

        if ( extent.writerIndex() + write_width > ShuffleJobOutput.EXTENT_SIZE ) {
            rollover();
        }

        extent.write( to_partition, key_value_length, key, value );
        
    }

    private void rollover() {
        extent = new ShuffleOutputExtent();
        extents.add( extent );
    }
    
}

class ShuffleOutputExtent {

    protected ChannelBuffer buff = ChannelBuffers.buffer( ShuffleJobOutput.EXTENT_SIZE );

    protected int count = 0;
    
    public void write( int to_partition, int length, byte[] key, byte[] value ) {

        buff.writeInt( to_partition );
        buff.writeInt( length );
        DefaultChunkWriter.write( buff, key, value );

        ++count;
        
    }

    public int writerIndex() {
        return buff.writerIndex();
    }
    
}

/**
 * Buffer output until it is ready to be sent in one CHUNK.
 * 
 */
class BufferedChannelBuffer {

    public List<ChannelBuffer> buffers = new ArrayList();

    private int length = 0;
    
    public void add( ChannelBuffer buff ) {
        buffers.add( buff );
        length += buff.writerIndex();
    }

    /**
     * Make sure we can accept this output buffer so that it's < 16384 but as
     * close as possible.
     */
    public boolean hasCapacity( ChannelBuffer buff ) {
        return length + buff.writerIndex() + RemoteChunkWriterClient.CHUNK_OVERHEAD > MAX_CHUNK_SIZE;
    }
    
}