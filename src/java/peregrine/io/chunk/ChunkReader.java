package peregrine.io.chunk;

import java.io.*;

public interface ChunkReader extends Closeable {

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
    public byte[] key() throws IOException;

    /**
     * Read the value from the current entry.
     *
     * Both key() and value() must be called before moving to the next item.
     */
    public byte[] value() throws IOException;

    /**
     * Return the number of key/value pairs in this ChunkReader.
     */
    public int size() throws IOException;

    /**
     * Close the ChunkReader.
     */
    @Override
    public void close() throws IOException;
    
}