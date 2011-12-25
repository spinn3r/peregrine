package peregrine.io.chunk;

import java.io.*;

import peregrine.*;
import peregrine.values.*;

/**
 * Write key/values to chunks.
 */
public interface ChunkWriter extends Closeable, Flushable {

    /**
     * Write a key value pair.  This is the main method for IO to a chunk.
     */
    public void write( StructReader key, StructReader value ) throws IOException;

    /**
     * Return the length of bytes of this chunk.
     */
    public long length() throws IOException;

    /**
     * Shutdown any pending IO without blocking.
     */
    public void shutdown() throws IOException;

}