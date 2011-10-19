
package peregrine.reduce.sorter;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.reduce.merger.*;

import org.jboss.netty.buffer.*;

public class ChannelBufferSorterIntermediate implements SorterIntermediate {

    ChannelBuffer buff;

    ChannelBufferChunkWriter chunkWriter = null;
    
    public ChannelBufferSorterIntermediate( ChannelBuffer buff ) {
        this.buff = buff;
    }

    @Override
    public ChunkWriter getChunkWriter() throws IOException {
        chunkWriter = new ChannelBufferChunkWriter( buff );
        return chunkWriter;
    }
    
    @Override
    public ChunkReader getChunkReader() throws IOException {

        ChannelBuffer region = buff.slice( 0, buff.writerIndex() );

        // we need to duplicate this because we can't let anyone write on these
        // bytes.
        region = region.duplicate();
        
        return new ChannelBufferChunkReader( region , chunkWriter.count );
        
    }
        
}

class ChannelBufferChunkWriter implements ChunkWriter {

    ChannelBuffer buff;

    int count = 0;
    
    public ChannelBufferChunkWriter( ChannelBuffer buff ) {
        this.buff = buff;
    }

   /**
     * Write a key value pair.  This is the main method for IO to a chunk.
     */
    @Override
    public void write( byte[] key, byte[] value ) throws IOException {
        buff.writeInt( key.length );
        buff.writeBytes( key );
        buff.writeInt( value.length );
        buff.writeBytes( value );
    }

    /**
     * Total number of items in this chunk writer.  Basically, a count of the
     * total number of key value pair writes done to this ChunkWriter.
     */
    @Override
    public int count() throws IOException {
        return count;
    }

    @Override
    public long length() throws IOException {
        return buff.writerIndex();
    }

    @Override
    public void close() throws IOException {
        // noop 
    }

}

class ChannelBufferChunkReader implements ChunkReader {

    ChannelBuffer buff;

    int size = 0;

    int idx = 0;
    
    public ChannelBufferChunkReader( ChannelBuffer buff, int size ) {
        this.buff = buff;
        this.size = size;
    }
    
    @Override
    public boolean hasNext() throws IOException {
        return idx < size;
    }

    @Override
    public byte[] key() throws IOException {
        return read();
    }

    @Override
    public byte[] value() throws IOException {
        ++idx;
        return read();
    }

    private byte[] read() throws IOException {
        int length = buff.readInt();
        byte[] data = new byte[ length ];
        buff.readBytes( data );
        return data;
    }
    
    @Override
    public int size() throws IOException {
        return size;
    }

    @Override
    public void close() throws IOException {
        //noop
    }

}