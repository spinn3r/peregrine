
package peregrine.io.async;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Basic functionality of an output stream.
 * 
 */
public abstract class BaseOutputStream extends OutputStream {

    public void write( byte[] b, int off, int len ) throws IOException {

        byte[] data = new byte[ len ];

        System.arraycopy( b, off, data, 0, len );

        write( data );
        
    }

    public void write( int b ) throws IOException {
        write( new byte[] { (byte)b } );
    }
    
    public void flush() throws IOException {

    }

}
