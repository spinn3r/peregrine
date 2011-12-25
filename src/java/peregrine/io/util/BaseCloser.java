package peregrine.io.util;

import java.io.*;
import java.util.*;

/**
 * Implements JDK 1.7 try-with-resources style closing for multiple Closeables.
 *
 * http://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
 */
public abstract class BaseCloser<T> {

    protected List<T> delegates = new ArrayList();

    private Throwable cause = null;

    private boolean executed = false;

    public BaseCloser() { }

    public BaseCloser( List<T> delegates ) {
        this.delegates = delegates;
    }
    
    public BaseCloser( T... delegates ) {
        add( delegates );
    }

    public void add( T delegate ) {
        delegates.add( delegate );
    }

    public void add( T... delegates ) {
        for( T current : delegates ) {
            this.delegates.add( current );
        }
    }

    public void setCause( Throwable cause ) {
        this.cause = cause;
    }

    protected boolean executed() {
        return executed;
    }
    
    protected void exec() throws GroupIOException {

        if ( executed )
            return;
        
        GroupIOException exc = null;

        if ( cause != null )
            exc = new GroupIOException( cause );
        
        for ( T current : delegates ) {

            if ( current == null )
                continue;
            
            try {

                onDelegate( current );

            } catch ( Throwable t ) {

                if ( exc == null ) {
                    exc = new GroupIOException( t );
                } else { 
                    exc.addSuppressed( t );
                }
                
            }

        }

        executed = true;
        
        if ( exc != null )
            throw exc;
        
    }

    protected abstract void onDelegate( T delegate ) throws IOException;
    
}
    