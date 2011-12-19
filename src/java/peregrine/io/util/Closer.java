package peregrine.io.util;

import java.io.*;
import java.util.*;

/**
 * Implements JDK 1.7 try-with-resources style closing for multiple Closeables.
 *
 * @see http://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
 */
public class Closer implements Closeable {

    private List<Closeable> closeables = new ArrayList();

    private Throwable cause = null;

    private boolean closed = false;

    public Closer() { }

    public Closer( List<Closeable> closeables ) {
        this.closeables = closeables;
    }
    
    public Closer( Closeable... closeables ) {
        add( closeables );
    }

    public void add( Closeable closeable ) {
        closeables.add( closeable );
    }

    public void add( Closeable... closeables ) {
        for( Closeable current : closeables ) {
            this.closeables.add( current );
        }
    }

    public void setCause( Throwable cause ) {
        this.cause = cause;
    }

    public boolean closed() {
        return closed;
    }
    
    @Override
    public void close() throws GroupIOException {

        if ( closed )
            return;
        
        GroupIOException exc = null;

        if ( cause != null )
            exc = new GroupIOException( cause );
        
        for ( Closeable current : closeables ) {

            if ( current == null )
                continue;
            
            try {

                current.close();

            } catch ( Throwable t ) {

                if ( exc == null ) {
                    exc = new GroupIOException( t );
                } else { 
                    exc.addSuppressed( t );
                }
                
            }

        }

        closed = true;
        
        if ( exc != null )
            throw exc;
        
    }
    
}
    