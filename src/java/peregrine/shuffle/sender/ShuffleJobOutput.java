package peregrine.shuffle.sender;

import java.io.*;
import java.util.concurrent.*;

import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.util.*;
import peregrine.io.chunk.*;
import com.spinn3r.log5j.Logger;

public class ShuffleJobOutput implements JobOutput, LocalPartitionReaderListener {

    protected Config config;
    protected String name;
    protected Partition partition;

    protected ShuffleJobOutputDelegate jobOutputDelegate;

    protected LocalPartitionReaderListener localPartitionReaderListener;

    public ShuffleJobOutput( Config config, Partition partition ) {
        this( config, "default", partition );
    }
        
    public ShuffleJobOutput( Config config, String name, Partition partition ) {

        this.config = config;
        this.name = name;
        this.partition = partition;
        
        //jobOutputDelegate = new ShuffleJobOutputBatched( this );
        jobOutputDelegate = new ShuffleJobOutputDirect( this );

        localPartitionReaderListener = (LocalPartitionReaderListener) jobOutputDelegate;
        
    }
    
    @Override
    public void emit( byte[] key , byte[] value ) {
        jobOutputDelegate.emit( key, value );
    }

    public void emit( int to_partition, byte[] key , byte[] value ) {
        jobOutputDelegate.emit( to_partition, key, value );
    }

    @Override 
    public void close() throws IOException {
        jobOutputDelegate.close();
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

}

