package maprunner.io;

import java.io.*;
import java.util.*;

import maprunner.*;
import maprunner.util.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.io.*;

/**
 * Read data from a partition from local storage.
 */
public class LocalPartitionReader {

    private String path = null;

    private List<ChunkReader> chunkReaders = new ArrayList();

    private Iterator<ChunkReader> iterator = null;

    /**
     * The current chunk reader.
     */
    private ChunkReader chunkReader = null;

    private Tuple t = null;
    
    public LocalPartitionReader( Partition partition,
                                 Host host, 
                                 String path ) throws IOException {

        List<File> chunks = LocalPartition.getChunkFiles( partition, host, path );

        for( File chunk : chunks ) {
            chunkReaders.add( new DefaultChunkReader( chunk ) );
        }

        iterator = chunkReaders.iterator();
        
    }

    /**
     * Used for clients that want to read from chunks directly.
     */
    public List<ChunkReader> getChunkReaders() {
        return chunkReaders;
    }
    
    public Tuple read() throws IOException {

        if ( chunkReader != null )
            t = chunkReader.read();

        if ( t == null && iterator.hasNext() ) {
            chunkReader = iterator.next();
            t = chunkReader.read();
        }

        return t;
        
    }

    public void close() throws IOException {

        if ( chunkReader != null )
            chunkReader.close();

    }
    
}