package peregrine.io;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.keys.*;
import peregrine.map.*;
import peregrine.shuffle.*;
import peregrine.util.*;
import peregrine.values.*;
import peregrine.io.chunk.*;

public class ShuffleJobOutput implements JobOutput, LocalPartitionReaderListener {

    private int partitions = 0;

    protected ChunkReference chunkRef = null;

    protected Shuffler shuffler = null;

    protected Config config;

    public ShuffleJobOutput( Config config ) {
        this( config, "default" );
    }
        
    public ShuffleJobOutput( Config config, String name ) {

        Membership partitionMembership = config.getPartitionMembership();

        this.partitions = partitionMembership.size();

        this.shuffler = Shuffler.getInstance( name );
        this.config = config;
        
    }
    
    @Override
    public void emit( byte[] key , byte[] value ) {

        Partition target_partition = Config.route( key, partitions, true );

        MapOutputIndex mapOutputIndex = shuffler.getMapOutputIndex( target_partition );
        
        mapOutputIndex.accept( chunkRef.global, key, value );

    }

    @Override 
    public void close() throws IOException {

    }

    @Override 
    public void onChunk( ChunkReference chunkRef ) {
        this.chunkRef = chunkRef;
    }

    @Override 
    public void onChunkEnd( ChunkReference ref ) { }
    
}

