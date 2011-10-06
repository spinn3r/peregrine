package peregrine.io;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;

/**
 * Delegates performing the actual IO to a given subsystem.  Local or remote. 
 */
public interface PartitionWriterDelegate extends PartitionWriter {

    public void rollover() throws IOException;

    public void erase() throws IOException;

    /**
     * Enable append mode.
     */
    public void setAppend() throws IOException;
    
}

