package peregrine.io.partition;

import java.io.*;
import peregrine.config.Config;
import peregrine.config.Host;
import peregrine.config.Partition;
import peregrine.http.ChannelBufferWritable;

/**
 * Delegates performing the actual IO to a given subsystem.  Local or remote. 
 */
public interface PartitionWriterDelegate {

    /**
     * Init the writer delegate with the given partition host and path.
     */
    public void init( Config config,
                      Partition partition,
                      Host host,
                      String path ) throws IOException;

    public void erase() throws IOException;

    /**
     * Enable append mode and return the chunk ID we should start writing to.
     */
    public int append() throws IOException;
    
    public ChannelBufferWritable newChunkWriter( int chunk_id ) throws IOException;

    public Host getHost();

}

