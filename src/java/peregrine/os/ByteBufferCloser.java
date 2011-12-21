package peregrine.os;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

import org.jboss.netty.buffer.*;

import peregrine.http.*;
import peregrine.util.netty.*;
import peregrine.io.util.*;
import peregrine.config.*;

import com.spinn3r.log5j.Logger;

/**
 * A closeable which is smart enough to work on byte buffers. 
 */
public class ByteBufferCloser implements Closeable {

    private ByteBuffer buff;
    
    public ByteBufferCloser( ByteBuffer buff ) {
        this.buff = buff;
    }
    
    @Override
    public void close() throws IOException {

        sun.misc.Cleaner cl = ((sun.nio.ch.DirectBuffer)buff).cleaner();

        if (cl != null) {
            cl.clean();
        }
        
    }

}
