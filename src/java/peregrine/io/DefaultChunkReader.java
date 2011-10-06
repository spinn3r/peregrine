package peregrine.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;

public class DefaultChunkReader implements ChunkReader {

    public static int BUFFER_SIZE = 16384;
    
    private File file = null;

    private TrackedInputStream input = null;

    private VarintReader varintReader = new VarintReader();

    private long length = -1;

    /**
     * number of key value pairs to deal with.
     */
    private int size = 0;
    
    public DefaultChunkReader( String path )
        throws IOException {

        this( new File( path ) );

    }

    public DefaultChunkReader( File file )
        throws IOException {
        
        this.file = file;

        //read from the given file.
        InputStream is = new FileInputStream( file );

        //and also buffer it
        is = new BufferedInputStream( is, BUFFER_SIZE );

        // and keep track of reads but also buffer the IO ...
        this.input = new TrackedInputStream( is );
        this.length = file.length();

        RandomAccessFile raf = new RandomAccessFile( file , "r" );

        try {
        
            raf.seek( file.length() - IntBytes.LENGTH );
            
            byte[] size_bytes = new byte[ IntBytes.LENGTH ];
            raf.read( size_bytes );
            
            setSize( IntBytes.toInt( size_bytes ) );

        } finally {
            
            raf.close();

        }
        
    }

    public DefaultChunkReader( byte[] data )
        throws IOException {

        this.length = data.length;

        byte[] size_bytes = new byte[ IntBytes.LENGTH ];
        System.arraycopy( data, data.length - IntBytes.LENGTH, size_bytes, 0, IntBytes.LENGTH );

        setSize( IntBytes.toInt( size_bytes ) );

        this.input = new TrackedInputStream( new ByteArrayInputStream( data ) );
        
    }

    private void setSize( int size ) throws IOException {

        if ( size < 0 ) {
            throw new IOException( "Invalid size: " + size );
        }
        
        this.size = size;
    }

    public boolean hasNext() throws IOException {

        if( this.input.getPosition() < this.length - IntBytes.LENGTH ) {
            return true;
        } else {
            return false;
        }

    }

    public byte[] key() throws IOException {
        return readBytes( varintReader.read( this.input ) );
    }

    public byte[] value() throws IOException {
        return readBytes( varintReader.read( this.input ) );
    }
    
    public void close() throws IOException {
        this.input.close();
    }

    public int size() throws IOException {
        return this.size;
    }
    
    /**
     * Dump this chunk to stdout.
     */
    public void dump() throws IOException {

        System.out.printf( "==== BEGIN DefaultChunkReader DUMP ==== \n" );
        
        while( hasNext() ) {

            System.out.printf( "key=%s, value=%s\n", Hex.encode( key() ), Hex.encode( value() ) );

        }

        System.out.printf( "==== END DefaultChunkReader DUMP ==== \n" );

    }
    
    private byte[] readBytes( int len ) throws IOException {

        byte[] data = new byte[len];
        input.read( data );
        return data;
        
    }

    public static void main( String[] args ) throws Exception {

        ChunkReader reader = new DefaultChunkReader( args[0] );

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
