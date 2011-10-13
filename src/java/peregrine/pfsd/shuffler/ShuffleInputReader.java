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

    private InputStream in;
    
    public ShuffleInputReader( String path, int partition ) throws IOException {

        this.path = path;
        this.partition = partition;

        // pull out the header information 

        File file = new File( path );

        // NOTE: we read directly from a FileInputStream here because we need to
        // use skip to jump over the preamble. This MAY be a slight performance
        // issue but I doubt it.  There are only 12 bytes per partition and if
        // the local partition has 25 partitions this is only 300 bytes and in
        // the worse case scenario we read 288 extra bytes.
        
        in = new FileInputStream( file );
        this.struct = new StructReader( in );

        // read the magic.
        byte[] magic = struct.read( new byte[ ShuffleOutputWriter.MAGIC.length ] );

        int size = struct.readInt();

        int start = -1;

        int point = ShuffleOutputWriter.MAGIC.length + IntBytes.LENGTH;
        
        for ( int i = 0; i < size; ++i ) {

            int part   = struct.readInt();
            int off    = struct.readInt();
            int count  = struct.readInt();

            point += IntBytes.LENGTH * 3;
            
            if ( part == partition ) {
                start      = off;
                this.count = count;
            }
            
        }

        if ( start == -1 )
            throw new IOException( "Unable to find start for part: " + partition );
    
        int skip = start - point;

        in.skip( skip );

        // now switched to buffered reads... 
        in = new BufferedInputStream( in , BUFFER_SIZE );
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
        
        if ( to_partition != this.partition )
           throw new IOException( "Read invalid partition data: " + to_partition );
        
        byte[] data = new byte[ len ];
        data = struct.read( data );
        
        ShufflePacket pack = new ShufflePacket( from_partition, from_chunk, to_partition, data );

        return pack;
        
    }
    
}
