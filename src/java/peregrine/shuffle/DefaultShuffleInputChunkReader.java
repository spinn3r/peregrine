package peregrine.shuffle;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.values.*;
import peregrine.io.async.*;
import peregrine.io.chunk.*;

import org.jboss.netty.buffer.*;

import com.spinn3r.log5j.Logger;

/**
 * Read shuffle data but in the ChunkReader interface so that we can use
 * ChunkSorter to manage the data and then reduce over it.
 * 
 */
public class DefaultShuffleInputChunkReader implements ShuffleInputChunkReader{

    ShuffleInputReader reader;

    ShufflePacket pack = null;

    VarintReader varintReader;

    String path;

    byte[] key = null;

    byte[] value = null;

    int partition;

    int key_offset;
    int key_length;

    int value_offset;
    int value_length;
    
    public DefaultShuffleInputChunkReader( String path , int partition ) throws IOException {

        this.path = path;
        this.partition = partition;
        this.reader = new ShuffleInputReader( path, partition );
        
        nextShufflePacket();
        
    }

    public boolean hasNext() throws IOException {

        // FIXME: hasNext shouldn't perform any state mutation
        
        while( true ) {

            if ( pack != null && pack.data.readerIndex() < pack.data.capacity() ) {

                this.key_length     = varintReader.read();
                this.key_offset     = pack.data.readerIndex();

                pack.data.readerIndex( pack.data.readerIndex() + key_length );
                
                this.value_length   = varintReader.read();
                this.value_offset   = pack.data.readerIndex();

                pack.data.readerIndex( pack.data.readerIndex() + value_length ); 

                return true;
                
            } else if ( nextShufflePacket() ) {

                // we need to read the next ... 
                continue;

            } else {
                return false;
            }

        }
            
    }

    private boolean nextShufflePacket() throws IOException {

        if ( reader.hasNext() ) {
            
            pack          = reader.next();
            varintReader  = new VarintReader( pack.data );
            pack.data.readerIndex( 0 );
            
            return true;
            
        } else {
            return false;
        }

    }

    public ShufflePacket getShufflePacket() {
        return pack;
    }
    
    public int keyOffset() {
        return key_offset;
    }

    public int keyLength() {
        return key_length;
    }
    
    public byte[] key() throws IOException {
        return readBytes( key_offset, key_length );
        
    }

    public byte[] value() throws IOException {
        return readBytes( value_offset, value_length );
    }
    
    public int size() throws IOException {
        return reader.getHeader().count;
    }

    public void close() throws IOException {
        reader.close();
    }

    public String toString() {
        return String.format( "%s:%s:%s" , getClass().getName(), path, partition );
    }

    public ShuffleInputReader getShuffleInputReader() {
        return reader;
    }
    
    private byte[] readBytes( int offset, int length ) throws IOException {

        byte[] data = new byte[ length ];
        pack.data.getBytes( offset, data );

        return data;
        
    }

    public static void main( String[] args ) throws Exception {

        DefaultShuffleInputChunkReader reader = new DefaultShuffleInputChunkReader( args[0], Integer.parseInt( args[1] ) );

        while( reader.hasNext() ) {

            System.out.printf( "%s = %s\n", Hex.encode( reader.key() ), Hex.encode( reader.value() ) );

        }
        
    }

}