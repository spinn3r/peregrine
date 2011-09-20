package maprunner;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import maprunner.util.*;
import maprunner.keys.*;
import maprunner.values.*;

public class ChunkReader {

    private File file = null;

    private TrackedInputStream is = null;

    private VarintReader varintReader = new VarintReader();

    private ChunkListener listener = null;
    
    public ChunkReader( String path, ChunkListener listener ) {
        this( new File( path ), listener );
    }

    public ChunkReader( File file, ChunkListener listener  ) {
        this.file = file;
        this.listener = listener;
    }
    
    public void read() throws IOException {

        FileInputStream fis = new FileInputStream( file );

        is = new TrackedInputStream( fis );
        
        while( is.getPosition() < file.length() ) {
            
            int key_length = readEntryLength();
            byte[] key_data = readBytes( key_length );
            
            int value_length = readEntryLength();
            byte[] value_data = readBytes( value_length );

            listener.onEntry( key_data, value_data );
                
        }
        
    }

    private byte[] readBytes( int len ) throws IOException {

        byte[] data = new byte[len];
        is.read( data );
        return data;
        
    }
    
    private int readEntryLength() throws IOException {

        byte[] buff = new byte[4];

        int varint_len = 0;;

        for( int i = 0; i < buff.length; ++i ) {
            byte b = (byte)is.read();
            buff[i] = b;

            if ( isLastVarintByte( b ) ) {
                varint_len = i + 1;
                break;
            }
            
        }

        byte[] vbuff = new byte[ varint_len ];
        System.arraycopy( buff, 0, vbuff, 0, varint_len );
        
        return varintReader.read( vbuff );
        
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

class TrackedInputStream extends BufferedInputStream {

    private int position = 0;

    public TrackedInputStream( InputStream is ) {
        super( is );
    }
    
    public int read() throws IOException {

        ++position;
        return super.read();
    }

    public int read( byte[] b,
                     int off,
                     int len )
        throws IOException {

        position += len;
        return super.read( b, off, len );

    }
    
    public int read(byte[] b) throws IOException {
        position += b.length;
        return super.read( b );
    }

    public int getPosition() {
        return position;
    }

}