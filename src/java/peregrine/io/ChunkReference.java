package peregrine.io;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.io.*;

/**
 * Read data from a partition from local storage.
 */
public class ChunkReference {

    public long global = -1;
    public int local = -1;

    public ChunkReference() {}

    public ChunkReference( long global, int local ) {
        this.global = global;
        this.local = local;
    }
    
}