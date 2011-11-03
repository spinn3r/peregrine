package peregrine.io.partition;

import java.io.*;

/**
 * Main PartitionWriter interface. 
 */
public interface PartitionWriter {

    public void write( byte[] key, byte[] value ) throws IOException;

    public void close() throws IOException;

    public String toString();
    
}

