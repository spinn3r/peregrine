
package peregrine.reduce.sorter;

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
import peregrine.reduce.merger.*;

import org.jboss.netty.buffer.*;

public class PersistentSorterIntermediate implements SorterIntermediate {

    private String path;
    
    public PersistentSorterIntermediate( String path ) {
        this.path = path;
    }

    @Override
    public ChunkWriter getChunkWriter() throws IOException {
        return new DefaultChunkWriter( new File( path ) );
    }
    
    @Override
    public ChunkReader getChunkReader() throws IOException {
        return new DefaultChunkReader( new File( path ) );
    }
        
}
