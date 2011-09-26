package maprunner;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import maprunner.util.*;
import maprunner.keys.*;
import maprunner.values.*;

public class ChunkReader {

    public static int BUFFER_SIZE = 16384;
    
    private File file = null;

    private TrackedInputStream input = null;

    private VarintReader varintReader = new VarintReader();

    private ChunkListener listener = null;

    private long length = -1;
    
    public ChunkReader( String path, ChunkListener listener )
        throws IOException {

        this( new File( path ), listener );

    }

    public ChunkReader( File file, ChunkListener listener  )
        throws IOException {
        
        this.file = file;
        this.listener = listener;

        //read from the given file.
        InputStream is = new FileInputStream( file );

        //and also buffer it
        is = new BufferedInputStream( is, BUFFER_SIZE );

        // and keep track of reads but also buffer the IO ...
        this.input = new TrackedInputStream( is );
        this.length = file.length();

    }

    public ChunkReader( byte[] data,
                        ChunkListener listener ) 
        throws IOException {

        this( data );
        this.listener = listener;
        
    }

    public ChunkReader( byte[] data )
        throws IOException {

        this.length = data.length;
        
        this.input = new TrackedInputStream( new ByteArrayInputStream( data ) );
        
    }

    /**
     * deprecated ... see readKeyValuePair
     */
    public void read() throws IOException {

        while( this.input.getPosition() < this.length ) {
            
            int key_length = readEntryLength();
            byte[] key_data = readBytes( key_length );
            
            int value_length = readEntryLength();
            byte[] value_data = readBytes( value_length );
            
            listener.onEntry( key_data, value_data );

        }

    }

    public KeyValuePair readKeyValuePair() throws IOException {

        if( this.input.getPosition() < this.length ) {
            
            int key_length = readEntryLength();
            byte[] key_data = readBytes( key_length );
            
            int value_length = readEntryLength();
            byte[] value_data = readBytes( value_length );

            return new KeyValuePair( key_data, value_data );
            
        } else {
            return null;
        }

    }

    private byte[] readBytes( int len ) throws IOException {

        byte[] data = new byte[len];
        input.read( data );
        return data;
        
    }
    
    private int readEntryLength() throws IOException {

        byte[] buff = new byte[4];

        //the length of the varint we are using...
        int varint_len = 0;;

        for( int i = 0; i < buff.length; ++i ) {
            byte b = (byte)this.input.read();
            buff[i] = b;

            if ( isLastVarintByte( b ) ) {
                varint_len = i + 1;
                break;
            }
            
        }

        byte[] vbuff = new byte[ varint_len ];
        System.arraycopy( buff, 0, vbuff, 0, varint_len );

        int result = varintReader.read( vbuff );

        return result;
        
    }
    
    private boolean isLastVarintByte( byte b ) {
        return (b >> 7) == 0;
    }

    public static void main( String[] args ) throws IOException {

        String path = args[0];

        final AtomicInteger tuples = new AtomicInteger();
        
        ChunkListener listener = new ChunkListener() {

                public void onEntry( byte[] key, byte[] value ) {
                    tuples.getAndIncrement();
                }

            };

        ChunkReader reader = new ChunkReader( path, listener );
        reader.read();
        
        System.out.printf( "%s has %,d tuples.\n", path, tuples.get() );
        
    }
    
}
