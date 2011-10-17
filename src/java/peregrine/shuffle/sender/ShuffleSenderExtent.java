package peregrine.shuffle.sender;

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
import peregrine.reduce.*;
import peregrine.util.*;
import peregrine.values.*;
import peregrine.io.chunk.*;
import peregrine.io.async.*;
import peregrine.pfs.*;

import com.spinn3r.log5j.Logger;

import static peregrine.pfsd.FSPipelineFactory.*;

public class ShuffleSenderExtent {

    protected ChannelBuffer buff = ChannelBuffers.buffer( ShuffleJobOutput.EXTENT_SIZE );

    protected int emits = 0;
    
    public void emit( int to_partition, int length, byte[] key, byte[] value ) {

        buff.writeInt( to_partition );
        buff.writeInt( length );
        DefaultChunkWriter.write( buff, key, value );

        ++emits;
        
    }

    public int writerIndex() {
        return buff.writerIndex();
    }
    
}
