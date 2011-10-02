package peregrine.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.lang.reflect.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.io.*;
import peregrine.shuffle.*;

public class ShuffleJobOutput implements JobOutput {

    private int partitions = 0;

    public long global_chunk_id = -1;

    public ShuffleJobOutput() {

        Map<Partition,List<Host>> partitionMembership = Config.getPartitionMembership();

        this.partitions = partitionMembership.size();
        
    }
    
    @Override
    public void emit( byte[] key , byte[] value ) {

        Shuffler shuffler = Shuffler.getInstance();
        
        Partition target_partition = Config.route( key, partitions, true );

        MapOutputIndex mapOutputIndex = shuffler.getMapOutputIndex( target_partition );
        
        mapOutputIndex.accept( global_chunk_id, key, value );

    }

    @Override 
    public void close() throws IOException {

    }
    
}

