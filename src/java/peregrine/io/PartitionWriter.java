package peregrine.io;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;

/**
 * Main PartitionWriter interface. 
 */
public interface PartitionWriter {

    public void write( byte[] key, byte[] value ) throws IOException;

    public void close() throws IOException;

    public String toString();
    
}

