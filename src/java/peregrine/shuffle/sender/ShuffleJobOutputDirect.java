package peregrine.shuffle.sender;

import java.io.*;
import java.util.concurrent.*;

import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.util.*;
import peregrine.io.chunk.*;
import com.spinn3r.log5j.Logger;

public class ShuffleJobOutputDirect implements JobOutput, LocalPartitionReaderListener {

    private ShuffleJobOutput parent;

    private ShuffleSender sender = null;
    
    public ShuffleJobOutputDirect( ShuffleJobOutput parent ) {
        this.parent = parent;
    }
    
    @Override
    public void emit( byte[] key , byte[] value ) {

        try {
            
            Partition target = parent.config.route( key );

            sender.emit( target.getId(), key, value );

        } catch ( ShuffleFailedException e ) {
            // this should cause the job to (correctly) fail
            throw new RuntimeException( e );
        }
        
    }


    @Override 
    public void onChunk( ChunkReference chunkRef ) {
        sender = new ShuffleSender( parent.config, parent.name, chunkRef );
    }

    @Override 
    public void onChunkEnd( ChunkReference chunkRef ) {
        
        try {
            sender.close();
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }

    }

    @Override 
    public void close() throws IOException {
        // redundant because of onChunkEnd 
    }

    @Override
    public String toString() {
        return String.format( "%s:%s", getClass().getSimpleName(), parent.name );
    }

}

