package peregrine.io;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.io.*;

/**
 * Write to a logical partition which is a stream of chunk files.... 
 */
public class LocalPartitionWriterDelegate implements PartitionWriterDelegate {

    private Partition partition;

    private Host host;

    private String path = null;

    private ChunkWriter chunkWriter = null;

    private int chunk_id = 0;
    
    public void init( Partition partition,
                      Host host,
                      String path ) throws IOException {

        this.partition = partition;
        this.host = host;
        this.path = path;
        
    }

    public void write( byte[] key_bytes, byte[] value_bytes )
        throws IOException {

        chunkWriter.write( key_bytes, value_bytes );
        
    }

    public void close() throws IOException {

        //close the last opened chunk writer.
        chunkWriter.close();        

    }

    public String toString() {
        return path;
    }

    // PartitionWriterDelegate

    public void setAppend() throws IOException {

        // FIXME: how do we setup append mode remotely ? This is going to be
        // even a bigger issue when you factor in that ALL of the
        // PartitionWriters could be remote.  ONE thing we could do is
        // include a nonce as the beginning chunk ID ... Right now some of
        // the probe operations where we read the file by ID wouldn't work
        // in this manner though AND the clocks would need to be
        // synchronized...hm.  ACTUALLY .. they won't ALL be non-local.  At
        // least ONE will be local.. actually... no.  Not during extracts
        // that run on the source.
        //
        
        // I could write a manifest file that I could just GET on the remote
        // end.
        
        List<File> chunks = LocalPartition.getChunkFiles( partition, host, path );

        // the chunk_id needs to be changed so that the append works.
        chunk_id = chunks.size();

        // I could do a HTTP HEAD on this remote resource to get back the
        // right information.

    }
    
    public void erase() throws IOException {

        List<File> chunks = LocalPartition.getChunkFiles( partition, host, path );

        for ( File chunk : chunks ) {
            
            if ( ! chunk.delete() )
                throw new IOException( "Unable to remove local chunk: " + chunk );
            
        }

    }

    public void rollover() throws IOException {

        if ( chunkWriter != null )
            chunkWriter.close();

        chunkWriter = newChunkWriter( chunk_id );
        
        ++chunk_id; // change the chunk ID now for the next file.
        
    }

    public long chunkLength() throws IOException {
        return chunkWriter.length();
    }
    
    private ChunkWriter newChunkWriter( int chunk_id ) throws IOException {

        String local = Config.getPFSPath( partition, host, path );

        String chunk_name = LocalPartition.getFilenameForChunkID( this.chunk_id );
        String chunk_path = new File( local, chunk_name ).getPath();

        return new LocalChunkWriter( chunk_path );

    }

}