package peregrine.pfsd.shuffler;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.values.*;
import peregrine.io.async.*;

import com.spinn3r.log5j.Logger;

/**
 * 
 */
public class ShuffleInputReader {

    private static final Logger log = Logger.getLogger();

    public static int BUFFER_SIZE = 8192;

    // header lookup information for partition and where to start reading.
    private Map<Integer,Integer> lookup = new HashMap();

    private String path;

    private StructReader struct;

    private int count;

    private int idx = 0;

    private int partition;
    
    public ShuffleInputReader( String path, int partition ) throws IOException {

        this.path = path;
        this.partition = partition;

        // pull out the header information 

        InputStream in;

        File file = new File( path );
        
        FileInputStream fis = new FileInputStream( file );
        in = new BufferedInputStream( fis , BUFFER_SIZE );

        this.struct = new StructReader( in );

        // read the magic.
        byte[] magic = struct.read( new byte[ ShuffleOutputWriter.MAGIC.length ] );

        int size = struct.readInt();

        int start = -1;
        
        for ( int i = 0; i < size; ++i ) {

            int part   = struct.readInt();
            int off    = struct.readInt();
            int count  = struct.readInt();

            if ( part == partition ) {
                start      = off;
                this.count = count;
            }
            
        }

        int point = ShuffleOutputWriter.MAGIC.length + IntBytes.LENGTH + (size * IntBytes.LENGTH * 2);
    
        int skip = start - point;

        fis.skip( skip );

        // create a new input as the previous one is now invalid.
        in = new BufferedInputStream( fis , BUFFER_SIZE );
        this.struct = new StructReader( in );

    }

    public boolean hasNext() throws IOException {
        return idx < count;
    }
    
    public ShufflePacket next() throws IOException {

        if ( idx >= count )
            return null;

        ++idx;
        
        int from_partition  = struct.readInt();
        int from_chunk      = struct.readInt();
        int to_partition    = struct.readInt();
        int len             = struct.readInt();

        byte[] data = new byte[ len ];

        data = struct.read( data );
        
        ShufflePacket pack = new ShufflePacket( from_partition, from_chunk, to_partition, data );

        return pack;
        
    }
    
}
