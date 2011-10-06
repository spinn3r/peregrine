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
public class LocalPartitionWriter {

    /**
     * Chunk size for rollover files.
     */
    public static long CHUNK_SIZE = 134217728;
    
    private String path = null;

    private int chunk_id = 0;

    private ChunkWriter chunkWriter = null;

    private Partition partition;

    private Host host;
    
    public LocalPartitionWriter( Partition partition,
                                 Host host,
                                 String path ) throws IOException {
        this( partition, host, path, false );
    }
        
    public LocalPartitionWriter( Partition partition,
                                 Host host,
                                 String path,
                                 boolean append ) throws IOException {

        this.path = path;
        this.partition = partition;
        this.host = host;

        if ( append == false ) {

            erase();

        } else {

            // FIXME: how do we setup append mode remotely ? This is going to be
            // even a bigger issue when you factor in that ALL of the
            // PartitionWriters could be remote.  ONE thing we could do is
            // include a nonce as the beginning chunk ID ... Right now some of
            // the probe operations where we read the file by ID wouldn't work
            // in this manner though AND the clocks would need to be
            // synchronized...hm.  ACTUALLY .. they won't ALL be non-local.  At
            // least ONE will be local.. actually... no.  Not during extracts
            // that run on the source.
            
            List<File> chunks = LocalPartition.getChunkFiles( partition, host, path );

            // the chunk_id needs to be changed so that the append works.
            chunk_id = chunks.size();

        }
        
        //create the first chunk...
        rollover();
        
    }

    public void write( byte[] key_bytes, byte[] value_bytes )
        throws IOException {

        chunkWriter.write( key_bytes, value_bytes );

        rolloverWhenNecessary();
        
    }

    public void close() throws IOException {
        //close the last opened partition...
        chunkWriter.close();        
    }

    public String toString() {
        return path;
    }

    protected ChunkWriter newChunkWriter( int chunk_id ) throws IOException {

        String local = Config.getPFSPath( partition, host, path );

        String chunk_name = LocalPartition.getFilenameForChunkID( this.chunk_id );
        String chunk_path = new File( local, chunk_name ).getPath();

        return new LocalChunkWriter( chunk_path );

    }

    protected void erase() throws IOException {

        List<File> chunks = LocalPartition.getChunkFiles( partition, host, path );

        for ( File chunk : chunks ) {
            
            if ( ! chunk.delete() )
                throw new IOException( "Unable to remove local chunk: " + chunk );
            
        }

    }
    
    private void rolloverWhenNecessary() throws IOException {

        if ( chunkWriter.length() > CHUNK_SIZE )
            rollover();
        
    }
    
    private void rollover() throws IOException {

        if ( chunkWriter != null )
            chunkWriter.close();

        chunkWriter = newChunkWriter( chunk_id );
        
        ++chunk_id; // change the chunk ID now for the next file.
        
    }

}