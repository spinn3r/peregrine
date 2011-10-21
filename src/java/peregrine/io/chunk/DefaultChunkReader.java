package peregrine.io.chunk;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;

import org.jboss.netty.buffer.*;

public class DefaultChunkReader implements ChunkReader {

    public static int BUFFER_SIZE = 8192;
    
    private File file = null;

    private InputStream input = null;

    private VarintReader varintReader;;

    private long length = -1;

    /**
     * number of key value pairs to deal with.
     */
    private int size = 0;

    /**
     * The current item we are reading from.
     */
    private int idx = 0;
    
    public DefaultChunkReader( File file )
        throws IOException {
        
        this.file = file;

        //read from the given file.
        InputStream is = new FileInputStream( file );

        //and also buffer it
        this.input = new BufferedInputStream( is, BUFFER_SIZE );

        this.varintReader = new VarintReader( this.input );
        
        // and keep track of reads but also buffer the IO ...
        this.length = file.length();

        RandomAccessFile raf = new RandomAccessFile( file , "r" );

        try {

            assertLength();
            raf.seek( this.length - IntBytes.LENGTH );
            
            byte[] size_bytes = new byte[ IntBytes.LENGTH ];
            raf.read( size_bytes );
            
            setSize( IntBytes.toInt( size_bytes ) );

        } finally {
            
            raf.close();

        }
        
    }

    public DefaultChunkReader( File file, ChannelBuffer buff )
        throws IOException {

        this.file = file;
        this.input = new ChannelBufferInputStream( buff );
        this.varintReader = new VarintReader( this.input );
        this.length = file.length();

        assertLength();
        setSize( buff.getInt( (int)(length - 4) ) );
        
    }
    
    public DefaultChunkReader( byte[] data )
        throws IOException {

        //FIXME: refactor this to use a ChannelBuffer ... via ChannelBuffers.wrapped()
        
        this.length = data.length;

        byte[] size_bytes = new byte[ IntBytes.LENGTH ];
        System.arraycopy( data, data.length - IntBytes.LENGTH, size_bytes, 0, IntBytes.LENGTH );

        setSize( IntBytes.toInt( size_bytes ) );

        this.input = new ByteArrayInputStream( data );
        this.varintReader = new VarintReader( this.input );

    }

    private void assertLength() throws IOException {
        if ( this.length < IntBytes.LENGTH )
            throw new IOException( String.format( "File %s is too short (%,d bytes)", file.getPath(), length ) );
    }

    private void setSize( int size ) throws IOException {

        if ( size < 0 ) {
            throw new IOException( "Invalid size: " + size );
        }

        this.size = size;
    }

    public boolean hasNext() throws IOException {

        if( idx < size ) {
            return true;
        } else {
            return false;
        }

    }

    public byte[] key() throws IOException {
        ++idx;
        return readBytes( varintReader.read() );
        
    }

    public byte[] value() throws IOException {
        return readBytes( varintReader.read() );
    }

    /**
     * skip the currnent key or value by reading the varint and the skipping
     * over it in the input stream.
     */
    public void skip() throws IOException {
        input.skip( varintReader.read() );        
    }
    
    public void close() throws IOException {
        this.input.close();
    }

    public int size() throws IOException {
        return this.size;
    }
    
    private byte[] readBytes( int len ) throws IOException {

        byte[] data = new byte[len];
        input.read( data );
        return data;
        
    }

    @Override
    public String toString() {

        if ( file != null )
            return file.getPath();

        return super.toString();
        
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
