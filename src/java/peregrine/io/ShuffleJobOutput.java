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

    protected int emitted = 0;
    
    public ShuffleJobOutput( Config config ) {
        this( config, "default" );
    }
        
    public ShuffleJobOutput( Config config, String name ) {

        this.config = config;
        this.name = name;
        
        this.partitionMembership = config.getPartitionMembership();

        this.partitions = partitionMembership.size();
        
    }
    
    @Override
    public void emit( byte[] key , byte[] value ) {

        Partition target = Config.route( key, partitions, true );

        int from_partition  = chunkRef.partition.getId();
        int from_chunk      = chunkRef.local;

        int to_partition    = target.getId();

        emit( to_partition, key, value );
        
    }

    protected void emit( int to_partition, byte[] key , byte[] value ) {
        shuffleOutput.write( to_partition, key, value );
        ++emitted;
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

            System.out.printf( "FIXME: on job end for shuffler... \n" );
            
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }

    }

    @Override 
    public void close() throws IOException {

        log.info( "Closing... emitted: %,d" , emitted );
        
        try {
            
            if ( future != null ) 
                future.get();
            
        } catch ( Exception e ) {
            throw new IOException( e );
        }
        
    }

}

