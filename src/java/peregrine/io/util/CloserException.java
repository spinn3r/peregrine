package peregrine.io.util;

import java.util.*;
import java.io.*;

/**
 * Implements JDK 1.7 try-with-resources style closing for multiple Closeables.
 *
 * http://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
 */
public class CloserException extends IOException {

    List<Throwable> suppressed = new ArrayList();

    private Throwable cause;
    
    public CloserException( Throwable cause ) {
        this.cause = cause;
        initCause( this.cause );
    }
    
    public void addSuppressed( Throwable t ) {
        suppressed.add( t );
    }
    
}
    