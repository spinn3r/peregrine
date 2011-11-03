package peregrine.io.partition;

import peregrine.io.chunk.*;

/**
 * Read data from a partition from local storage.
 */
public interface LocalPartitionReaderListener {

    /**
     * Called when we find a new chunk to process.
     */
    public void onChunk( ChunkReference ref );

    public void onChunkEnd( ChunkReference ref );

}