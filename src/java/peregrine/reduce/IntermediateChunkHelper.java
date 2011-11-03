
package peregrine.reduce;

import java.io.*;
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
