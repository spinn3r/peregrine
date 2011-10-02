package peregrine.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.io.*;

public class MapOutputBuffer {

    private long chunk_id = 0;

    private ByteArrayOutputStream out = null;

    private ChunkWriter writer = null;
    
    public MapOutputBuffer( long chunk_id ) throws IOException {
        this.chunk_id = chunk_id;
        this.out = new ByteArrayOutputStream();
        this.writer = new ChunkWriter( out );
    }
    
    public void accept( byte[] key, byte[] value ) throws IOException {
        writer.write( key, value );
    }

    public ChunkReader getChunkReader() throws IOException {

        // make sure we are closed when being called from the reducer... 
        this.writer.close();        

        return new DefaultChunkReader( out.toByteArray() );
        
    }
    
}
