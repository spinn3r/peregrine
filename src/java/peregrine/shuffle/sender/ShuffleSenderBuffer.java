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

    private ShuffleSenderExtent extent = null;

    protected List<ShuffleSenderExtent> extents = new ArrayList();

    protected int partitions;

    protected ChunkReference chunkRef = null;

    protected String name = null;

    protected boolean flushing = false;

    protected int emits = 0;

    protected long length = 0;

    protected Map<Integer,AtomicInteger> partitionCount = new HashMap();
    
    public ShuffleSenderBuffer( Config config, ChunkReference chunkRef, String name ) {

        this.chunkRef = chunkRef;
        this.name = name;

        for( Partition part : config.getMembership().getPartitions() ) {
            partitionCount.put( part.getId(), new AtomicInteger() );
        }
        
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
            IntBytes.LENGTH +
            IntBytes.LENGTH +
            key_value_length
            ;

        length += emit_width;
        
        if ( extent.writerIndex() + emit_width > ShuffleJobOutput.EXTENT_SIZE ) {
            rollover();
        }

        extent.emit( to_partition, key_value_length, key, value );

        // bump up the number of items on this partition.
        partitionCount.get( to_partition ).getAndIncrement();

        ++emits;
        
    }

    private void rollover() {

        extent = new ShuffleSenderExtent();
        extents.add( extent );
    }
    
}
