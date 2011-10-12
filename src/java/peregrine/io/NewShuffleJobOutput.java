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

public class NewShuffleJobOutput implements JobOutput, LocalPartitionReaderListener {

    private static final Logger log = Logger.getLogger();

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
    
    public NewShuffleJobOutput() {
        this( "default" );
    }
        
    public NewShuffleJobOutput( String name ) {

        this.partitionMembership = Config.getPartitionMembership();

        this.partitions = partitionMembership.size();

        this.shuffler = ShufflerFactory.getInstance( name );

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

    }

    @Override 
    public void onChunk( ChunkReference chunkRef ) {

        this.chunkRef = chunkRef;

        this.shuffleOutput = new ShuffleOutput( chunkRef );
        
    }

    @Override 
    public void onChunkEnd( ChunkReference ref ) {

        try {
            
            // now close out the previous output and start writing it to cients on
            // the network.

            log.info( "Closing shuffle job output for chunk: %s", ref );

            Map<Partition,OutputStream> clients = new HashMap();

            Membership membership = Config.getPartitionMembership();
            
            Set<Partition> partitions = membership.getPartitions();
            
            for( Partition part : partitions ) {

                List<Host> hosts = membership.getHosts( part );

                List<OutputStream> output = new ArrayList();
                
                for( Host host : hosts ) {

                    URI uri = new URI( String.format( "http://%s:%s/shuffle/default/from-partition/%s/from-chunk/%s/to-partition/%s",
                                                      host.getName(),
                                                      host.getPort(),
                                                      ref.partition.getId(),
                                                      ref.local,
                                                      part.getId() ) );

                    RemoteChunkWriterClient client = new RemoteChunkWriterClient( uri );

                    output.add( client );
                    
                }

                clients.put( part, new MultiOutputStream( output ) );
                
            }

            // FIXME: now read the data and write it to all clients .. man this is hard...

            // FIXME: now close all clients...
            
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

    List<ShuffleOutputExtent> extents = new ArrayList();

    ShuffleOutputExtent extent = null;

    private int partitions;

    private ChunkReference chunkRef = null;
    
    public ShuffleOutput( ChunkReference chunkRef ) {

        this.chunkRef = chunkRef;
        
        rollover();

    }
    
    public void write( int to_partition, byte[] key, byte[] value ) {

        // the max width that this write could consume.  2 ints for the
        // partition and the width of the value and then the length of the key
        // and the lenght of the value + two ints for the varints.

        int length =
            VarintWriter.sizeof( key.length ) +
            key.length +
            VarintWriter.sizeof( value.length ) +
            value.length
            ;
        
        int write_width =
            IntBytes.LENGTH +
            IntBytes.LENGTH +
            length
            ;

        if ( extent.writerIndex() + write_width > NewShuffleJobOutput.EXTENT_SIZE ) {
            rollover();
        }

        extent.write( to_partition, length, key, value );
        
    }

    private void rollover() {
        extent = new ShuffleOutputExtent();
        extents.add( extent );
    }
    
}

class ShuffleOutputExtent {

    ChannelBuffer buff = ChannelBuffers.buffer( NewShuffleJobOutput.EXTENT_SIZE );

    public void write( int to_partition, int length, byte[] key, byte[] value ) {

        buff.writeInt( to_partition );
        buff.writeInt( length );
        DefaultChunkWriter.write( buff, key, value );
        
    }

    public int writerIndex() {
        return buff.writerIndex();
    }
    
}

