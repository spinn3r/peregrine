package peregrine.os;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

/**
 * 
 *
 *
 */
public class MappedFile {

    protected FileInputStream in;
    
    protected FileChannel channel;

    protected MappedByteBuffer map;

    protected long offset = 0;

    protected long length = 0;

    protected boolean lock = false;

    protected MemLock memLock = null;

    protected FileChannel.MapMode mode;

    protected File file;
    
    public MappedFile( File file, FileChannel.MapMode mode ) throws IOException {

        this.file = file;
        this.in = new FileInputStream( file );
        this.channel = in.getChannel();
        this.mode = mode;
        this.length = file.length();
        
    }

    public MappedByteBuffer map() throws IOException {

        if ( map == null ) {

            if ( lock ) 
                memLock = new MemLock( file, in.getFD(), offset, length );

            map = channel.map( mode, offset, length );
            
        }

        return map;
        
    }

    public boolean getLock() { 
        return this.lock;
    }

    public void setLock( boolean lock ) { 
        this.lock = lock;
    }
    
    public void close() throws IOException {

       if ( memLock != null )
            memLock.release();

       System.out.printf( "FIXME0 \n" );
       close( map );
       
       System.out.printf( "FIXME1 \n" );
       channel.close();
       
       System.out.printf( "FIXME2 \n" );
       in.close();

       System.out.printf( "FIXME3 \n" );
       
    }

    @SuppressWarnings("all")
    private void close( MappedByteBuffer map ) {

        sun.misc.Cleaner cl = ((sun.nio.ch.DirectBuffer)map).cleaner();

        if (cl != null) {
            cl.clean();
        }

    }
    
}