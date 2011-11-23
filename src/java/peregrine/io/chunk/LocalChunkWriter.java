package peregrine.io.chunk;

import java.io.*;

/**
 * Export chunks are used in both the Extract phase of ETL jobs with
 * ExtractWriter to write data to individual partitions and chunks AND with map
 * jobs so that we can spool data to disk in the from the mappers directly to
 * the R partition files BEFORE we sort the output so that we can perform reduce
 * on each (K,V...) pair.
 */
public class LocalChunkWriter implements ChunkWriter {

    private DefaultChunkWriter delegate = null;
    
    public LocalChunkWriter( String path ) throws IOException {

        File file = new File( path );
        
        // make sure the parent directories exist.
        new File( file.getParent() ).mkdirs();

        this.delegate = new DefaultChunkWriter( file );
        
    }

    @Override
    public void write( byte[] key, byte[] value ) throws IOException {
        delegate.write( key, value );        
    }

    @Override
    public int count() throws IOException {
        return delegate.count();
    }
    
    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void shutdown() throws IOException {
        // noop on local 
    }

    @Override
    public long length() throws IOException {
        return delegate.length();
    }
    
}