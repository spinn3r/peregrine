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
import peregrine.pfsd.shuffler.*;

public class NewShuffleJobOutput implements JobOutput, LocalPartitionReaderListener {

    private int partitions = 0;

    protected ChunkReference chunkRef = null;

    protected peregrine.pfsd.shuffler.Shuffler shuffler = null;

    protected Membership partitionMembership;
    
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

        // Figure out who hosts this data:
        //List<Host> hosts = partitionMembership.getHosts( target );

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

