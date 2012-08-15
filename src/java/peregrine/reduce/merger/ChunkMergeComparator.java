package peregrine.reduce.merger;

import java.util.*;

import peregrine.reduce.*;

public class ChunkMergeComparator implements Comparator<MergeQueueEntry> {

    private ReduceComparator delegate = null;

    public ChunkMergeComparator( ReduceComparator delegate ) {
        this.delegate = delegate;
    }

    public int compare( MergeQueueEntry k0, MergeQueueEntry k1 ) {
        return delegate.compare( k0, k1 );
    }

}

