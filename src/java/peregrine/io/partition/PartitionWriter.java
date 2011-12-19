package peregrine.io.partition;

import java.io.*;

import peregrine.values.*;

/**
 * Main PartitionWriter interface. 
 */
public interface PartitionWriter extends Closeable, Flushable {

    public void write( StructReader key, StructReader value ) throws IOException;

    public void shutdown() throws IOException;

    public void close() throws IOException;

    /**
     * Total lengh of this file (bytes written) to this partition writer..
     */
    public long length();

    public String toString();

}

