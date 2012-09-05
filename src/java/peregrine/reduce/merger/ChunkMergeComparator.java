package peregrine.reduce.merger;

import java.util.*;

import peregrine.sort.*;

public class ChunkMergeComparator implements Comparator<MergeQueueEntry> {

    private SortComparator delegate = null;

    public ChunkMergeComparator( SortComparator delegate ) {
        this.delegate = delegate;
    }

    public int compare( MergeQueueEntry k0, MergeQueueEntry k1 ) {
        return delegate.compare( k0, k1 );
    }

}

