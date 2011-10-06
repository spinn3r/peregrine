
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

public class IntermediateChunkHelper {

    ByteArrayOutputStream out;

    public LocalChunkWriter getLocalChunkWriter() throws IOException {

        this.out = new ByteArrayOutputStream();
        return new LocalChunkWriter( out );
        
    }

    public ChunkReader getChunkReader() throws IOException {
        return new DefaultChunkReader( out.toByteArray() );
    }
    
}
