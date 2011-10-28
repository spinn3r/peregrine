package peregrine.shuffle.sender;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
    
import org.jboss.netty.buffer.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.keys.*;
import peregrine.map.*;
import peregrine.reduce.*;
import peregrine.util.*;
import peregrine.values.*;
import peregrine.io.chunk.*;
import peregrine.io.async.*;
import peregrine.pfs.*;

import com.spinn3r.log5j.Logger;

import static peregrine.pfsd.FSPipelineFactory.*;

/**
 * 
 */
public class ShuffleSenderBuffer {

    private static final Logger log = Logger.getLogger();

    /**
     * Write in 2MB chunks at ~100MB output this is only 2MB extra memory
     * potentially wasted which would be at most 2%.
     */
    public static final int EXTENT_SIZE = 2097152;

    /**
     * How often to log extent creation.  
     */
    public static final int EXTENT_LOG_INTERVAL = 25;
    
    private ShuffleSenderExtent extent = null;

    protected List<ShuffleSenderExtent> extents = new ArrayList();

    protected int partitions;

    protected ChunkReference chunkRef = null;

    protected String name = null;

    protected boolean flushing = false;

    protected int emits = 0;

    protected long length = 0;
    
    public ShuffleSenderBuffer( Config config, ChunkReference chunkRef, String name ) {

        this.chunkRef = chunkRef;
        this.name = name;

        rollover();

    }
    
    public void emit( int to_partition, byte[] key, byte[] value ) {

        // the max width that this emit could consume.  2 ints for the
        // partition and the width of the value and then the length of the key
        // and the lenght of the value + two ints for the varints.

        int key_value_length =
            VarintWriter.sizeof( key.length ) +
            key.length +
            VarintWriter.sizeof( value.length ) +
            value.length
            ;
        
        int emit_width =
            ShuffleSenderExtent.HEADER_SIZE +
            key_value_length
            ;

        length += emit_width;
        
        if ( extent.writerIndex() + emit_width > EXTENT_SIZE ) {
            rollover();
        }

        extent.emit( to_partition, key_value_length, key, value );

        ++emits;
        
    }

    public long capacity() {
        return extents.size() * EXTENT_SIZE;
    }
    
    private void rollover() {

        extent = new ShuffleSenderExtent( EXTENT_SIZE );
        extents.add( extent );

        if ( (extents.size() + 1) % EXTENT_LOG_INTERVAL == 0 ) {
            log.info( "Now using %,d bytes for buffer %s", capacity(), toString() );
        }

    }

    @Override
    public String toString() {
        return String.format( "%s:%s", name, super.toString() );
    }
    
}
