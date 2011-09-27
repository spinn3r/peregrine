package maprunner.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import maprunner.*;
import maprunner.util.*;
import maprunner.keys.*;
import maprunner.values.*;

public class DefaultChunkReader implements ChunkReader {

    private static int SIZE_BYTE_ARRAY_LENGTH = 4;
    
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
        raf.seek( file.length() - SIZE_BYTE_ARRAY_LENGTH );

        byte[] size_bytes = new byte[ SIZE_BYTE_ARRAY_LENGTH ];
        raf.read( size_bytes );

        System.out.printf( "FIXME: read from : %s\n", Hex.encode( size_bytes ) );

        this.size = IntBytes.toInt( size_bytes );

        raf.close();
        
    }

    public DefaultChunkReader( byte[] data )
        throws IOException {

        this.length = data.length;

        byte[] size_bytes = new byte[ SIZE_BYTE_ARRAY_LENGTH ];
        System.arraycopy( data, data.length - SIZE_BYTE_ARRAY_LENGTH, size_bytes, 0, SIZE_BYTE_ARRAY_LENGTH );

        this.size = IntBytes.toInt( size_bytes );

        this.input = new TrackedInputStream( new ByteArrayInputStream( data ) );
        
    }

    public Tuple read() throws IOException {

        if( this.input.getPosition() < this.length - SIZE_BYTE_ARRAY_LENGTH ) {
            
            byte[] key     = readBytes( varintReader.read( this.input ) );
            byte[] value   = readBytes( varintReader.read( this.input ) );

            return new Tuple( key, value );
            
        } else {
            return null;
        }

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
        
        while( true ) {
            
            Tuple tuple = read();

            if ( tuple == null )
                break;

            System.out.printf( "key=%s, value=%s\n", Hex.encode( tuple.key ), Hex.encode( tuple.value ) );

        }

        System.out.printf( "==== END DefaultChunkReader DUMP ==== \n" );

    }
    
    private byte[] readBytes( int len ) throws IOException {

        byte[] data = new byte[len];
        input.read( data );
        return data;
        
    }

    public static void main( String[] args ) throws IOException {

    }
    
}
