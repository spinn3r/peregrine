package peregrine.io;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
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
import peregrine.pfsd.shuffler.*;

public class NewShuffleJobOutput implements JobOutput, LocalPartitionReaderListener {

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

        shuffleOutput.write( to_partition, value );
        
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
    public void onChunkEnd( ChunkReference ref ) { }
    
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
    
    public void write( int to_partition, byte[] data ) {

        int width = data.length + (IntBytes.LENGTH * 2);

        if ( extent.writerIndex() + width > NewShuffleJobOutput.EXTENT_SIZE ) {
            rollover();
        }

        extent.write( to_partition, data );
        
    }

    private void rollover() {
        extent = new ShuffleOutputExtent();
        extents.add( extent );
    }
    
}

class ShuffleOutputExtent {

    ChannelBuffer buff = ChannelBuffers.buffer( NewShuffleJobOutput.EXTENT_SIZE );
    
    public void write( int to_partition, byte[] data ) {

        buff.writeInt( to_partition );
        buff.writeInt( data.length );
        buff.writeBytes( data );
        
    }

    public int writerIndex() {
        return buff.writerIndex();
    }
    
}