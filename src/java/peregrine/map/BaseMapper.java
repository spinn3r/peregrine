package peregrine.map;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.shuffle.*;
import peregrine.io.*;

public abstract class BaseMapper {

    public int partitions = 0;

    private long global_chunk_id = -1;

    public Input input = null;
    
    public void init() { }

    /**
     * Perform mapper cleanup.  close open files, etc.
     */
    public void cleanup() { }

    public Input getInput() {
        return this.input;
    }

    /**
     * Init a map task. This also allows us to find the input files we're
     * interacting with.  Merge task will use multiple files and merge them but
     * you should generally never have to work with them directly.
     */
    public void setInput( Input input ) {
        this.input = input;
    }
    
    public final void emit( byte[] key,
                            byte[] value ) {

        // TODO: the emit logic shouldn't go here ideally and should be moved to
        // a dedicated class and the Mapper should be clean.

        Partition target_partition = Config.route( key, partitions, true );

        MapOutputIndex mapOutputIndex = ShuffleManager.getMapOutputIndex( target_partition );
        
        mapOutputIndex.accept( global_chunk_id, key, value );
        
    }

    /**
     * Set the number of partitions we're running with.
     */
    public void setPartitions( int partitions ) {
        this.partitions = partitions;
    }
    
    public void setGlobalChunkId( long global_chunk_id ) {
        this.global_chunk_id = global_chunk_id;
    }

}
