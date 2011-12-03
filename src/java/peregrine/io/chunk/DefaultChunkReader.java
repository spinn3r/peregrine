package peregrine.io.chunk;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;

import peregrine.*;
import peregrine.os.*;
import peregrine.util.*;
import peregrine.util.primitive.IntBytes;

import org.jboss.netty.buffer.*;

public class DefaultChunkReader implements ChunkReader, Closeable {

    public static byte[] MAGIC_PREFIX =
        new byte[] { (byte)'P', (byte)'C', (byte)'0' };

    public static byte[] MAGIC_RAW = 
        new byte[] { (byte)'R', (byte)'A', (byte)'W' };

    public static byte[] MAGIC_CRC32 = 
        new byte[] { (byte)'C', (byte)'3', (byte)'2' };

    private File file = null;

    private ChannelBuffer buff = null;

    private VarintReader varintReader;;

    /**
     * Length in bytes of the input.
     */
    private long length = -1;

    /**
     * number of key value pairs to deal with.
     */
    private int size = 0;

    /**
     * The current item we are reading from.
     */
    private int idx = 0;

    private MappedFile mappedFile;

    private boolean closed = false;
    
    public DefaultChunkReader( File file )
        throws IOException {

        mappedFile = new MappedFile( file , "r" );
        
        buff = mappedFile.map();
      
        init( buff );
        
    }

    public DefaultChunkReader( File file, ChannelBuffer buff )
        throws IOException {

        this.file = file;
        init( buff );

    }
    
    public DefaultChunkReader( byte[] data )
        throws IOException {
                 
        init( ChannelBuffers.wrappedBuffer( data ) );

    }

    public DefaultChunkReader( ChannelBuffer buff )
        throws IOException {

        init( buff );
        
    }

    private void init( ChannelBuffer buff )
        throws IOException {

        this.buff = buff;
        this.length = buff.writerIndex();
        this.varintReader = new VarintReader( buff );
        
        assertLength();
        setSize( buff.getInt( buff.writerIndex() - IntBytes.LENGTH ) );

    }
    
    @Override
    public boolean hasNext() throws IOException {

        if( idx < size ) {
            return true;
        } else {
            return false;
        }

    }

    @Override
    public byte[] key() throws IOException {
        ++idx;
        return readEntry();
    }

    @Override
    public byte[] value() throws IOException {
        return readEntry();
    }

    @Override
    public void close() throws IOException {

        if ( closed )
            return;
        
        if ( mappedFile != null )
            mappedFile.close();

        closed = true;
        
    }

    @Override
    public int size() throws IOException {
        return this.size;
    }

    @Override
    public String toString() {
        return String.format( "file: %s, length (in bytes): %,d, size: %,d", file, length, size );
    }

    private void assertLength() throws IOException {
        if ( this.length < IntBytes.LENGTH )
            throw new IOException( String.format( "File %s is too short (%,d bytes)", file.getPath(), length ) );
    }

    private void setSize( int size ) throws IOException {

        if ( size < 0 ) {
            throw new IOException( String.format( "Invalid size: %s (%s)", size, toString() ) );
        }

        this.size = size;
    }

    /**
     * Skip the currnent key or value by reading the length and the skipping
     * over it in the input stream.
     */
    public void skip() throws IOException {
        buff.readerIndex( buff.readerIndex() + varintReader.read() );
    }

    private byte[] readEntry() throws IOException {

        try {

            int len = varintReader.read();
            return readBytes( len );
            
        } catch ( Throwable t ) {
            throw new IOException( "Unable to parse: " + toString() , t );
        }
        
    }
    
    private byte[] readBytes( int len ) throws IOException {

        byte[] data = new byte[len];
        buff.readBytes( data );
        return data;
        
    }
    
    public static void main( String[] args ) throws Exception {

        ChunkReader reader = new DefaultChunkReader( new File( args[0] ) );

        Key key = null;
        Value value = null;
        
        if ( args.length > 1 ) {

            key   = (Key)  Class.forName( args[1] ).newInstance();
            value = (Value)Class.forName( args[2] ).newInstance();
            
        }
        
        while( reader.hasNext() ) {

            if ( key == null ) {

                System.out.printf( "%s = %s\n", Hex.encode( reader.key() ), Hex.encode( reader.value() ) );

            } else {

                key.fromBytes( reader.key() );
                value.fromBytes( reader.value() );
                
                System.out.printf( "%s = %s\n", key, value );

            }

        }
        
    }
    
}
