package peregrine.pfsd.shuffler;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.values.*;
import peregrine.io.async.*;
import peregrine.io.chunk.*;

import com.spinn3r.log5j.Logger;

/**
 * Read shuffle data but in the ChunkReader interface so that we can use
 * ChunkSorter to manage the data and then reduce over it.
 * 
 */
public class ShuffleInputChunkReader implements ChunkReader {

    ShuffleInputReader reader;

    boolean hasNext = false;

    ShufflePacket pack = null;

    int idx = 0;

    VarintReader varintReader;

    InputStream is = null;

    public byte[] key = null;

    public byte[] value = null;

    public ShuffleInputChunkReader( String path , int partition ) throws IOException {
        
        reader = new ShuffleInputReader( path, partition );

        nextShufflePacket();
        
    }

    @Override
    public boolean hasNext() throws IOException {

        while( true ) {

            if ( idx < pack.data.length ) {

                key   = readBytes( varintReader.read() );
                value = readBytes( varintReader.read() );

                idx += VarintWriter.sizeof( key.length ) +
                       key.length + 
                       VarintWriter.sizeof( value.length ) +
                       value.length
                    ;

                break;
                
            } else if ( nextShufflePacket() ) {

                // we need to read the next ... 
                continue;

            } else {
                hasNext = false;
                break;
            }

        }

        return hasNext;
            
    }

    private byte[] readBytes( int len ) throws IOException {

        byte[] data = new byte[len];
        is.read( data );
        return data;
        
    }

    private boolean nextShufflePacket() throws IOException {

        if ( reader.hasNext() ) {

            pack          = reader.next();
            is            = new ByteArrayInputStream( pack.data );
            varintReader  = new VarintReader( is );
            idx           = 0;
            hasNext       = true;

            return true;
            
        } else {
            return false;
        }

    }

    @Override
    public byte[] key() throws IOException {
        return key;
    }

    @Override
    public byte[] value() throws IOException {
        return value;
    }
    
    @Override
    public int size() throws IOException {
        throw new IOException();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

}