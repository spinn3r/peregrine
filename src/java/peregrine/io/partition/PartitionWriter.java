package peregrine.io.partition;

import java.io.*;

/**
 * Main PartitionWriter interface. 
 */
public interface PartitionWriter {

    public void write( byte[] key, byte[] value ) throws IOException;

    public void shutdown() throws IOException;

    public void close() throws IOException;

    /**
     * Total lengh of this file (bytes written) to this partition writer..
     */
    public long length();

    public String toString();

}

