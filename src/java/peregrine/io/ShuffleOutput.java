package peregrine.io;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.net.*;
import java.util.concurrent.*;

import org.jboss.netty.buffer.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.keys.*;
import peregrine.map.*;
import peregrine.shuffle.*;
import peregrine.util.*;
import peregrine.values.*;
import peregrine.io.chunk.*;
import peregrine.io.async.*;
import peregrine.pfs.*;
import peregrine.pfsd.shuffler.*;

import com.spinn3r.log5j.Logger;

import static peregrine.pfsd.FSPipelineFactory.*;

/**
 * 
 */
public class ShuffleOutput {

    private static final Logger log = Logger.getLogger();

    private ShuffleOutputExtent extent = null;

    protected List<ShuffleOutputExtent> extents = new ArrayList();

    protected int partitions;

    protected ChunkReference chunkRef = null;

    protected String name = null;

    protected boolean flushing = false;
    
    public ShuffleOutput( ChunkReference chunkRef, String name ) {

        System.out.printf( "FIXME: 16 creating new shuffle output for: %s\n", name );
        
        this.chunkRef = chunkRef;
        this.name = name;
        
        rollover();

    }
    
    public void write( int to_partition, byte[] key, byte[] value ) {

        // the max width that this write could consume.  2 ints for the
        // partition and the width of the value and then the length of the key
        // and the lenght of the value + two ints for the varints.

        int key_value_length =
            VarintWriter.sizeof( key.length ) +
            key.length +
            VarintWriter.sizeof( value.length ) +
            value.length
            ;
        
        int write_width =
            IntBytes.LENGTH +
            IntBytes.LENGTH +
            key_value_length
            ;

        if ( extent.writerIndex() + write_width > ShuffleJobOutput.EXTENT_SIZE ) {
            System.out.printf( "ROLLING OVER because we ran out of space\n" );
            rollover();
        }

        extent.write( to_partition, key_value_length, key, value );

        System.out.printf( "FIXME: GOT AN EXTENT WRITE \n" );

    }

    private void rollover() {

        System.out.printf( "FIXME11 ROLLING\n" );
        extent = new ShuffleOutputExtent();
        extents.add( extent );
    }
    
}
