
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

import com.spinn3r.log5j.Logger;

public class PersistentSorterIntermediate implements SorterIntermediate {

    private static final Logger log = Logger.getLogger();

    private String path;
    
    public PersistentSorterIntermediate( String path ) {
        this.path = path;
    }

    @Override
    public ChunkWriter getChunkWriter() throws IOException {

        log.info( "Writing temporary sort file %s", path );

        return new DefaultChunkWriter( new File( path ) );
    }
    
    @Override
    public ChunkReader getChunkReader() throws IOException {
        return new DefaultChunkReader( new File( path ) );
    }
        
}
