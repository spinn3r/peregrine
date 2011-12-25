package peregrine.shuffle.sender;

import java.io.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.util.*;
import peregrine.io.chunk.*;
import peregrine.values.*;

import com.spinn3r.log5j.Logger;

public class ShuffleJobOutputDirect extends ShuffleJobOutputBase implements Closeable, Flushable {

    private static final Logger log = Logger.getLogger();
    
    private ShuffleJobOutput parent;

    private ShuffleSender sender = null;
    
    public ShuffleJobOutputDirect( ShuffleJobOutput parent ) {
        this.parent = parent;
    }
    
    @Override
    public void emit( StructReader key , StructReader value ) {
            
        Partition target = parent.config.partition( key );
        
        emit( target.getId(), key, value );
                    
    }

    @Override
    public void emit( int to_partition, StructReader key , StructReader value ) {

        try {
            sender.emit( to_partition, key, value );
        } catch ( ShuffleFailedException e ) {
            // this should cause the job to (correctly) fail
            throw new RuntimeException( e );
        }

    }

    private void rollover( ChunkReference chunkRef ) {
        closeWithUncheckedException();
        sender = new ShuffleSender( parent.config, parent.name, chunkRef );
    }
    
    @Override 
    public void onChunk( ChunkReference chunkRef ) {
        rollover( chunkRef );
    }

    @Override 
    public void onChunkEnd( ChunkReference chunkRef ) {
        rollover( chunkRef );
    }

    @Override 
    public void flush() throws IOException {

        if ( sender != null ) {
            sender.flush();
            length += sender.length();
        }

    }

    @Override 
    public void close() throws IOException {

        if ( sender != null ) {
            sender.close();
            length += sender.length();
            sender = null;
        }
            
    }

    private void closeWithUncheckedException() {

        try {
            close();
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }

    }
    
    @Override
    public String toString() {
        return String.format( "%s:%s", getClass().getSimpleName(), parent.name );
    }

}

