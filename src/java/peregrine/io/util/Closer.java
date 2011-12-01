package peregrine.io.util;

import java.io.*;

/**
 * Implements JDK 1.7 try-with-resources style closing for multiple Closeables.
 *
 * http://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
 */
public class Closer {

    public static void close( Closeable... closeables ) throws CloserException {
        close( null, closeables );
    }

    public static void close( Throwable cause , Closeable... closeables ) throws CloserException {

        CloserException exc = null;

        if ( cause != null )
            exc = new CloserException( cause );
        
        for ( Closeable current : closeables ) {

            if ( current == null )
                continue;
            
            try {

                current.close();

            } catch ( Throwable t ) {

                if ( exc == null ) {
                    exc = new CloserException( t );
                } else { 
                    exc.addSuppressed( t );
                }
                
            }

        }

        if ( exc != null )
            throw exc;
        
    }

}
    