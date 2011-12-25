package peregrine.shuffle.sender;

import java.io.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.util.*;
import peregrine.io.chunk.*;

import com.spinn3r.log5j.Logger;

public class ShuffleJobOutput
    implements JobOutput, LocalPartitionReaderListener, Closeable, Flushable {

    private static final Logger log = Logger.getLogger();

    protected Config config;
    protected String name;
    protected Partition partition;

    protected ShuffleJobOutputDelegate jobOutputDelegate;

    protected LocalPartitionReaderListener localPartitionReaderListener;

    private long started = System.currentTimeMillis();

    private int emits = 0;
    
    public ShuffleJobOutput( Config config, Partition partition ) {
        this( config, "default", partition );
    }
        
    public ShuffleJobOutput( Config config, String name, Partition partition ) {

        this.config = config;
        this.name = name;
        this.partition = partition;
        
        jobOutputDelegate = new ShuffleJobOutputDirect( this );

        localPartitionReaderListener = (LocalPartitionReaderListener) jobOutputDelegate;
        
    }
    
    @Override
    public void emit( StructReader key , StructReader value ) {
        jobOutputDelegate.emit( key, value );
        ++emits;
    }

    public void emit( int to_partition, StructReader key , StructReader value ) {
        jobOutputDelegate.emit( to_partition, key, value );
        ++emits;
    }

    @Override 
    public void flush() throws IOException {
        jobOutputDelegate.flush();
    }

    @Override 
    public void close() throws IOException {

        jobOutputDelegate.close();

        long now = System.currentTimeMillis();

        long duration = (now-started);

        long throughput = -1;

        try {
            throughput = (long)((length() / (double)duration) * 1000);
        } catch ( Throwable t ) { }

        log.info( "Shuffled %,d entries (%,d bytes) from %s in %s %,d ms with throughput %,d b/s",
                  emits, length(), partition, this, duration, throughput );

    }

    @Override 
    public void onChunk( ChunkReference chunkRef ) {
        localPartitionReaderListener.onChunk( chunkRef );
    }

    @Override 
    public void onChunkEnd( ChunkReference chunkRef ) {
        localPartitionReaderListener.onChunkEnd( chunkRef );
    }

    @Override
    public String toString() {
        return jobOutputDelegate.toString();
    }

    public long length() {
        return jobOutputDelegate.length();
    }
    
}

