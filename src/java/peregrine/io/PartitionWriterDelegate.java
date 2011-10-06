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

    /**
     * Init the writer delegate with the given partition host and path.
     */
    public void init( Partition partition,
                      Host host,
                      String path ) throws IOException;
    
    public void rollover() throws IOException;

    public void erase() throws IOException;

    /**
     * Enable append mode.
     */
    public void setAppend() throws IOException;

    /**
     * Returns the length of the currently opened chunk in this delegate.
     */
    public long chunkLength() throws IOException;
    
}

