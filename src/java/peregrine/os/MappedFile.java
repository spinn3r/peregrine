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
public class MappedFile extends ByteBufferBackedChannelBuffer {

    FileInputStream in;
    FileChannel channel;

    MappedByteBuffer map;
    
    public MappedFile( File file, FileChannel.MapMode mode, long offset, long length ) {

        in = new FileInputStream( file );
        channel = new in.getChannel();
        map = channel.map( mode, offset, length );

    }

    public MappedByteBuffer getMappedByteBuffer() {
        return map;
    }
    
    public void close() throws IOException {

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