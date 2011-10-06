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

public class ShuffleJobOutput implements JobOutput, LocalPartitionReaderListener {

    private int partitions = 0;

    protected ChunkReference chunkRef = null;

    protected Shuffler shuffler = null;
    
    public ShuffleJobOutput() {
        this( "default" );
    }
        
    public ShuffleJobOutput( String name ) {

        Membership partitionMembership = Config.getPartitionMembership();

        this.partitions = partitionMembership.size();

        this.shuffler = Shuffler.getInstance( name );
        
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

