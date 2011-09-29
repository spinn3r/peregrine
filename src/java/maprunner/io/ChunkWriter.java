package maprunner.io;

import java.io.*;
import java.util.*;

import maprunner.*;
import maprunner.util.*;
import maprunner.keys.*;
import maprunner.values.*;

/**
 * Export chunks are used in both the Extract phase of ETL jobs with
 * ExtractWriter to write data to individual partitions and chunks AND with map
 * jobs so that we can spool data to disk in the from the mappers directly to
 * the R partition files BEFORE we sort the output so that we can perform reduce
 * on each (K,V...) pair.
 */
public class ChunkWriter {

    public static int BUFFER_SIZE = 16384;
    
    private String path = null;

    private VarintWriter varintWriter = new VarintWriter();

    private OutputStream out = null;

    private int size = 0;

    protected long length = 0;
    
    public ChunkWriter( String path ) throws IOException {
        this.path = path;

        // make sure the parent directories exist.
        new File( new File( this.path ).getParent() ).mkdirs();
        
        this.out = new BufferedOutputStream( new FileOutputStream( this.path ), BUFFER_SIZE );
        
    }

    public ChunkWriter( OutputStream out ) throws IOException {
        this.out = out;
    }

    public void write( byte[] key, byte[] value )
        throws IOException {

        write( varintWriter.write( key.length ) );
        write( key );

        write( varintWriter.write( value.length ) );
        write( value );

        ++size;

    }

    private void write( byte[] data ) throws IOException {
        out.write( data );
        length += data.length;
    }
    
    public void close() throws IOException {
        
        // last four bytes store the number of items.
        out.write( IntBytes.toByteArray( size ) );
        out.close();
        
    }
    
}