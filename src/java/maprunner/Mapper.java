package maprunner;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.shuffle.*;

public abstract class Mapper {

    private int nr_partitions = 0;

    private long chunk_id = -1;
    
    public abstract void map( byte[] key, byte[] value );

    public void init( int nr_partitions ) {

        this.nr_partitions = nr_partitions;

    }
    
    public void emit( byte[] key, byte[] value ) {

        // TODO: the emit logic shouldn't go here ideally and should be moved to
        // a dedicated class

        Partition target_partition = Config.route( key, nr_partitions, true );

        MapOutputIndex mapOutputIndex = ShuffleManager.getMapOutputIndex( target_partition );
        
        mapOutputIndex.accept( chunk_id, key, value );

    }

    public void onChunkStart( long chunk_id ) {
        this.chunk_id = chunk_id;
    }

    public void onChunkEnd( long chunk_id ) {
    }

    /**
     * Perform mapper cleanup.  close open files, etc.
     */
    public void cleanup( Partition partition ) {

        //TODO: we should probably keep track of the partitions that have seen
        //this data before we send them the close() message.
        
        //ShuffleManager.cleanup( partition );

    }
    
}
