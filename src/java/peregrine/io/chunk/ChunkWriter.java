package peregrine.io.chunk;

import java.io.*;
import peregrine.values.*;

public interface ChunkWriter extends Closeable {

    /**
     * Write a key value pair.  This is the main method for IO to a chunk.
     */
    public void write( StructReader key, StructReader value ) throws IOException;

    /**
     * Total number of items in this chunk writer.  Basically, a count of the
     * total number of key value pair writes done to this ChunkWriter.
     */

    public int count() throws IOException;

    public long length() throws IOException;

    public void shutdown() throws IOException;

    @Override
    public void close() throws IOException;

}