package maprunner;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.shuffle.*;

public class Mapper {

    public int nr_partitions = 0;

    private long global_chunk_id = -1;
    
    public void init( int nr_partitions ) {
        this.nr_partitions = nr_partitions;
    }

    //TODO: global_chunk_id should NOT be exposed in map but we had to include
    //it for now for technical reasons.
    public void map( byte[] key,
                     byte[] value ) {}

    public void emit( byte[] key,
                      byte[] value ) {

        // TODO: the emit logic shouldn't go here ideally and should be moved to
        // a dedicated class and the Mapper should be clean.

        Partition target_partition = Config.route( key, nr_partitions, true );

        MapOutputIndex mapOutputIndex = ShuffleManager.getMapOutputIndex( target_partition );
        
        mapOutputIndex.accept( global_chunk_id, key, value );
        
    }

    public void setGlobalChunkId( long global_chunk_id ) {
        this.global_chunk_id = global_chunk_id;
    }
    
    /**
     * Perform mapper cleanup.  close open files, etc.
     */
    public void cleanup( Partition partition ) {

    }
    
}
