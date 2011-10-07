package peregrine.io.partition;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.io.*;
import peregrine.io.async.*;
import peregrine.io.chunk.*;

import com.spinn3r.log5j.*;

/**
 * Write to a logical partition which is a stream of chunk files.... 
 */
public class LocalPartitionWriterDelegate extends BasePartitionWriterDelegate {

    private static final Logger log = Logger.getLogger();

    public void init( Partition partition,
                      Host host,
                      String path ) throws IOException {

        this.partition = partition;
        this.host = host;
        this.path = path;
        
    }

    @Override
    public String toString() {
        return path;
    }

    // PartitionWriterDelegate

    @Override
    public int append() throws IOException {

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

        return chunks.size();

    }
    
    @Override
    public void erase() throws IOException {

        List<File> chunks = LocalPartition.getChunkFiles( partition, host, path );

        for ( File chunk : chunks ) {
            
            if ( ! chunk.delete() )
                throw new IOException( "Unable to remove local chunk: " + chunk );
            
        }

    }

    @Override
    public OutputStream newChunkWriter( int chunk_id ) throws IOException {

        // FIXME: move this to LocalPartition.getChunkFile
        
        String local = Config.getPFSPath( partition, host, path );

        String chunk_name = LocalPartition.getFilenameForChunkID( chunk_id );
        String chunk_path = new File( local, chunk_name ).getPath();

        log.info( "Creating new local chunk writer: %s" , chunk_path );

        return LocalChunkWriter.getOutputStream( chunk_path );
        
    }

}