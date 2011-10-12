package peregrine.io.chunk;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.io.*;

/**
 * Read data from a partition from local storage.
 */
public class ChunkReference {

    public long global = -1;

    public int local = -1;

    public Partition partition = null;
    
    public ChunkReference() {}

    public ChunkReference( long global,
                           int local ) {
        
        this.global = global;
        this.local = local;
        
    }

    /**
     * Used when generating chunk references for tasks.
     */
    public ChunkReference( Partition partition ) {
        this.partition = partition;
    }

    /**
     * Increment the chunk reference during a read.  This bumps up the local
     * chunk ID by one and then uses the partition as a prefix to update global.
     */
    public void incr() {

        ++local;
        
        long prefix = (long)partition.getId() * 1000000000;

        this.global = prefix + local;

    }

    public String toString() {
        return String.format( "%s local=%06d", partition, local );
    }
    
}