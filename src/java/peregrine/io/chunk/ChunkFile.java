package peregrine.io.chunk;

import java.io.*;

/**
 * Used for chunk file manipulation (both local and remote).
 */
public interface ChunkFile {

    /**
     * Delete an entire chunk file and all the files under it.
     */
    public void delete() throws IOException;
    
}