package peregrine.io.chunk;

import java.io.*;
import peregrine.values.*;

public interface ChunkReader {

    /**
     * Return true if there is a next item.  Initially the reader is positioned
     * on the first item so that hasNext() returns true and you can then call
     * key() and value()
     */
    public boolean hasNext() throws IOException;

    /**
     * Read the key from the current entry.
     *
     * Both key() and value() must be called before moving to the next item.
     */
    public StructReader key() throws IOException;

    /**
     * Read the value from the current entry.
     *
     * Both key() and value() must be called before moving to the next item.
     */
    public StructReader value() throws IOException;

    /**
     * Return the number of key/value pairs in this ChunkReader.
     */
    public int size() throws IOException;

    /**
     * Close the ChunkReader.
     */
    public void close() throws IOException;
    
}