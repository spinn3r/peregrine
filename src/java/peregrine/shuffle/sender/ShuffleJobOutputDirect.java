package peregrine.shuffle.sender;

import java.io.*;
import java.util.concurrent.*;

import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.util.*;
import peregrine.io.chunk.*;
import com.spinn3r.log5j.Logger;

public class ShuffleJobOutputDirect implements ShuffleJobOutputDelegate {

    private ShuffleJobOutput parent;

    private ShuffleSender sender = null;
    
    public ShuffleJobOutputDirect( ShuffleJobOutput parent ) {
        this.parent = parent;
    }
    
    @Override
    public void emit( byte[] key , byte[] value ) {
            
        Partition target = parent.config.route( key );
        
        emit( target.getId(), key, value );
                    
    }

    @Override
    public void emit( int to_partition, byte[] key , byte[] value ) {

        try {
            sender.emit( to_partition, key, value );
        } catch ( ShuffleFailedException e ) {
            // this should cause the job to (correctly) fail
            throw new RuntimeException( e );
        }

    }

    @Override 
    public void onChunk( ChunkReference chunkRef ) {

        try {
            close();
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }

        sender = new ShuffleSender( parent.config, parent.name, chunkRef );
    }

    @Override 
    public void onChunkEnd( ChunkReference chunkRef ) {}

    @Override 
    public void close() throws IOException {

        if ( sender != null ) {
            sender.close();
        }
            
    }

    @Override
    public String toString() {
        return String.format( "%s:%s", getClass().getSimpleName(), parent.name );
    }

}

