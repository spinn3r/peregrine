package peregrine.shuffle.sender;

import java.io.*;
import java.util.concurrent.*;

import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.util.*;
import peregrine.io.chunk.*;
import com.spinn3r.log5j.Logger;

public class ShuffleJobOutput implements JobOutput, LocalPartitionReaderListener {

    private static final Logger log = Logger.getLogger();

    private static ExecutorService executors =
        Executors.newFixedThreadPool( 1, new DefaultThreadFactory( ShuffleJobOutput.class) );

    public static boolean DISABLED = false;
    
    protected ChunkReference chunkRef = null;

    protected ShuffleSenderBuffer shuffleSenderBuffer;

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

        chunkRef.partition.getId();
        int to_partition    = target.getId();

        emit( to_partition, key, value );
        
    }

    protected void emit( int to_partition, byte[] key , byte[] value ) {

        if ( DISABLED ) /* FIXME: remove this check before we ship */
            return;

        shuffleSenderBuffer.emit( to_partition, key, value );
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

        this.shuffleSenderBuffer = new ShuffleSenderBuffer( config, chunkRef, name );
        
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

    @Override
    public String toString() {
        return String.format( "ShuffleSenderBuffer:%s", name );
    }
    
    private void flush() throws IOException {
        flush( false );
    }
    
    private void flush( boolean force ) throws IOException {

        try {

            if ( future != null )
                future.get();

            if ( shuffleSenderBuffer != null ) {

                // only flush every 100MB written (or so) and also have a forced
                // flush on close...
                boolean trigger = force || shuffleSenderBuffer.length > DefaultPartitionWriter.CHUNK_SIZE;
                
                if ( trigger ) {
                    future = executors.submit( new ShuffleSenderFlushCallable( config, shuffleSenderBuffer ) );
                }

            }

        } catch ( Exception e ) {
            throw new IOException( e );
        }

    }
    
}

