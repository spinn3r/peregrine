package maprunner;

import java.io.*;
import java.util.*;

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

    private String path = null;

    private VarintWriter varintWriter = new VarintWriter();

    private FileOutputStream out = null;
    
    public ChunkWriter( String path ) throws IOException {
        this.path = path;

        new File( new File( this.path ).getParent() ).mkdirs();

        this.out = new FileOutputStream( this.path );
    }

    public void write( byte[] key_bytes, byte[] value_bytes )
        throws IOException {

        out.write( varintWriter.write( key_bytes.length ) );
        out.write( key_bytes );

        out.write( varintWriter.write( value_bytes.length ) );
        out.write( value_bytes );

    }

    public void close() throws IOException {
        out.close();        
    }
    
}