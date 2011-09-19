package maprunner;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.shuffle.*;

public abstract class Mapper {

    private int nr_shards = 0;

    private long chunk_id = -1;
    
    public abstract void map( byte[] key, byte[] value );

    public void init( int nr_shards ) {

        this.nr_shards = nr_shards;

    }
    
    public void emit( byte[] key, byte[] value ) {

        // FIXME: the emit logic shouldn't go here ideally and should be moved to
        // a dedicated system.

        int shard = Config.route( key, nr_shards, true );
        
        ShuffleManager.accept( shard, key, value );
        
    }

    public void onChunkStart( long chunk_id ) {
    }

    public void onChunkEnd( long chunk_id ) {
    }

    /**
     * Perform mapper cleanup.  close open files, etc.
     */
    public void cleanup( Partition partition ) {

        //TODO: we should probably keep track of the shards that have seen this
        //data before we send them the close() message.

        ShuffleManager.cleanup( partition );

    }
    
}
