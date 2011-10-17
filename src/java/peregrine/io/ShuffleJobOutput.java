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

import static peregrine.pfsd.FSPipelineFactory.*;

import com.spinn3r.log5j.Logger;

public class ShuffleJobOutput implements JobOutput, LocalPartitionReaderListener {

    private static final Logger log = Logger.getLogger();

    private static ExecutorService executors =
        Executors.newFixedThreadPool( 1, new DefaultThreadFactory( ShuffleJobOutput.class) );

    /**
     * Write in 2MB chunks at ~100MB output this is only 2MB extra memory
     * potentially wasted which would be at most 2%.
     */
    public static final int EXTENT_SIZE = 2097152;
    
    protected ChunkReference chunkRef = null;

    protected Shuffler shuffler = null;

    protected ShuffleOutput shuffleOutput;

    protected String name;

    protected Future future = null;

    protected Config config;

    protected int emits = 0;
    
    public ShuffleJobOutput( Config config ) {
        this( config, "default" );
    }
        
    public ShuffleJobOutput( Config config, String name ) {

        this.config = config;
        this.name = name;

    }
    
    @Override
    public void emit( byte[] key , byte[] value ) {

        Partition target = config.route( key, true );

        int from_partition  = chunkRef.partition.getId();
        int from_chunk      = chunkRef.local;

        int to_partition    = target.getId();

        emit( to_partition, key, value );
        
    }

    protected void emit( int to_partition, byte[] key , byte[] value ) {
        shuffleOutput.emit( to_partition, key, value );
        ++emits;
    }

    @Override 
    public void onChunk( ChunkReference chunkRef ) {

        this.chunkRef = chunkRef;

        try {
            flush();
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }

        this.shuffleOutput = new ShuffleOutput( chunkRef, name );
        
    }

    @Override 
    public void onChunkEnd( ChunkReference chunkRef ) { }

    @Override 
    public void close() throws IOException {

        log.info( "Closing %s... emits: %,d" , name, emits );

        // the first flush will trigger pending output to be async written to disk.
        flush( true );

        // the second flush will block until the prev finishs and then not do
        // anything else as no more emits are present.
        flush( true );
        
    }

    private void flush() throws IOException {
        flush( false );
    }
    
    private void flush( boolean force ) throws IOException {

        try {

            // FIXME: only flush every 100MB written (or so) and also have a
            // forced flush on close... 
            
            if ( future != null )
                future.get();

            if ( shuffleOutput != null ) {

                boolean trigger = force || shuffleOutput.length > DefaultPartitionWriter.CHUNK_SIZE;
                
                if ( trigger ) {
                    future = executors.submit( new ShuffleFlushCallable( config, shuffleOutput ) );
                }

            }

        } catch ( Exception e ) {
            throw new IOException( e );
        }

    }
    
}

