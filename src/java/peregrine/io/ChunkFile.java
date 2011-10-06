package peregrine.io;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.io.*;

/**
 * Used for chunk file manipulation (both local and remote).
 */
public interface ChunkFile {

    /**
     * Delete an entire chunk file and all the files under it.
     */
    public void delete() throws IOException;
    
}