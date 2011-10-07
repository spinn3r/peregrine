
package peregrine.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.io.*;
import peregrine.io.chunk.*;

public class IntermediateChunkHelper {

    ByteArrayOutputStream out;

    public ChunkWriter getChunkWriter() throws IOException {

        this.out = new ByteArrayOutputStream();
        return new DefaultChunkWriter( out );
        
    }

    public ChunkReader getChunkReader() throws IOException {
        return new DefaultChunkReader( out.toByteArray() );
    }
    
}
