
package peregrine.reduce.merger;

import peregrine.io.chunk.*;

public class MergeQueueEntry {

    public byte[] key;
    public byte[] value;
    
    protected MergerPriorityQueue queue = null;

    protected ChunkReader reader = null;

}

