package peregrine.map;

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

class ShuffleJobOutput implements JobOutput {

    private int partitions = 0;

    protected long global_chunk_id = -1;

    public ShuffleJobOutput( int partitions ) {
        this.partitions = partitions;
    }
    
    @Override
    public void emit( byte[] key , byte[] value ) {

        Partition target_partition = Config.route( key, partitions, true );

        MapOutputIndex mapOutputIndex = ShuffleManager.getMapOutputIndex( target_partition );
        
        mapOutputIndex.accept( global_chunk_id, key, value );

    }

    @Override 
    public void close() throws IOException {

    }
    
}

