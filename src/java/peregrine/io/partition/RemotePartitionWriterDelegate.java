package peregrine.io.partition;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.io.*;
import peregrine.io.chunk.*;

/**
 * Write to a logical partition which is a stream of chunk files.... 
 */
public class RemotePartitionWriterDelegate extends BasePartitionWriterDelegate {

    @Override
    public int append() throws IOException {
        throw new IOException( "not implemented yet" );
    }
    
    @Override
    public void erase() throws IOException {
        throw new IOException( "not implemented yet" );
    }

    @Override
    public ChunkWriter newChunkWriter( int chunk_id ) throws IOException {

        String local = Config.getPFSPath( partition, host, path );

        String chunk_name = LocalPartition.getFilenameForChunkID( chunk_id );
        String chunk_path = new File( local, chunk_name ).getPath();

        return new LocalChunkWriter( chunk_path );

    }

}