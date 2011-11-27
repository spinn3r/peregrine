package peregrine.os;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

/**
 * 
 *
 * @version $Rev: 2206M $, $Date: 2010-11-09 15:04:10 +0900 (Tue, 09 Nov 2010) $
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
    
    public MappedFile( File file, FileChannel.MapMode mode ) throws IOException {

        this.in = new FileInputStream( file );
        this.channel = in.getChannel();
        this.mode = mode;
        this.length = file.length();
        
    }

    public MappedByteBuffer map() throws IOException {

        if ( map == null ) {

            if ( lock ) 
                memLock = new MemLock( in.getFD(), offset, length );

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
        
        close( map );
        
        channel.close();

        in.close();

    }

    private void close( MappedByteBuffer map ) {

        sun.misc.Cleaner cl = ((sun.nio.ch.DirectBuffer)map).cleaner();

        if (cl != null) {
            cl.clean();
        }

    }
    
}