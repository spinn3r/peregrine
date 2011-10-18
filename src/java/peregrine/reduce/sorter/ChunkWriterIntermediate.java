
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

public class ChunkWriterIntermediate extends DefaultChunkWriter {

    public ChunkWriterIntermediate( int length ) throws IOException {
        super( new ByteArrayOutputStream( length ) );
    }

    public ChunkWriterIntermediate() throws IOException {
        this( 512 ) ;
    }

    public ChunkReader getChunkReader() throws IOException {

        close();
        
        ByteArrayOutputStream bos = (ByteArrayOutputStream)out;

        // FIXME ChannelBuffers would be WAY better because I don't have to call
        // toByteArray each time.
        return new DefaultChunkReader( bos.toByteArray() );

    }
    
}