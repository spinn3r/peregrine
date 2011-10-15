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

public class ShuffleOutputExtent {

    protected ChannelBuffer buff = ChannelBuffers.buffer( ShuffleJobOutput.EXTENT_SIZE );

    protected int count = 0;

    /**
     * 
     * 
     *
     */
    public ShuffleOutputExtent() {
        System.out.printf( "FIXME10 new extent created: %s\n", this );

        new Exception().printStackTrace();
    }

    public void write( int to_partition, int length, byte[] key, byte[] value ) {

        buff.writeInt( to_partition );
        buff.writeInt( length );
        DefaultChunkWriter.write( buff, key, value );

        ++count;

        System.out.printf( "FIXME12 GOT ONE EXTENT WRITE: %s\n", this );
        
    }

    public int writerIndex() {
        return buff.writerIndex();
    }
    
}
