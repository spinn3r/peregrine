package maprunner.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import maprunner.*;
import maprunner.util.*;
import maprunner.keys.*;
import maprunner.values.*;

public class DefaultChunkReader implements ChunkReader {

    public static int BUFFER_SIZE = 16384;
    
    private File file = null;

    private TrackedInputStream input = null;

    private VarintReader varintReader = new VarintReader();

    private ChunkListener listener = null;

    private long length = -1;
    
    public DefaultChunkReader( String path, ChunkListener listener )
        throws IOException {

        this( new File( path ), listener );

    }

    public DefaultChunkReader( File file, ChunkListener listener  )
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
    
    public DefaultChunkReader( byte[] data,
                               ChunkListener listener ) 
        throws IOException {

        this( data );
        this.listener = listener;
        
    }

    public DefaultChunkReader( byte[] data )
        throws IOException {

        this.length = data.length;
        
        this.input = new TrackedInputStream( new ByteArrayInputStream( data ) );
        
    }

    public Tuple read() throws IOException {

        if( this.input.getPosition() < this.length ) {
            
            int key_length = varintReader.read( this.input );
            byte[] key_data = readBytes( key_length );
            
            int value_length = varintReader.read( this.input );
            byte[] value_data = readBytes( value_length );

            return new Tuple( key_data, value_data );
            
        } else {
            return null;
        }

    }

    public void close() throws IOException {
        this.input.close();
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

        String path = args[0];

        final AtomicInteger tuples = new AtomicInteger();
        
        ChunkListener listener = new ChunkListener() {

                public void onEntry( byte[] key, byte[] value ) {
                    tuples.getAndIncrement();
                }

            };

        DefaultChunkReader reader = new DefaultChunkReader( path, listener );
        reader.read();
        
        System.out.printf( "%s has %,d tuples.\n", path, tuples.get() );
        
    }
    
}
