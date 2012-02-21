package peregrine.reduce.merger;

import java.util.*;

import peregrine.reduce.*;

public class ChunkMergeComparator implements Comparator<MergeQueueEntry> {

    //private DepthBasedKeyComparator delegate = new DepthBasedKeyComparator();
    private FullKeyComparator delegate = new FullKeyComparator();
    
    public int compare( MergeQueueEntry k0, MergeQueueEntry k1 ) {
        return delegate.compare( k0.keyAsByteArray, k1.keyAsByteArray );
    }

}

