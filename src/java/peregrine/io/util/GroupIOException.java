package peregrine.io.util;

import java.util.*;
import java.io.*;

/**
 * Implements JDK 1.7 try-with-resources style closing for multiple Closeables /
 * Flushables with an exception that includes all exceptions.
 *
 * http://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
 */
public class GroupIOException extends IOException {

    List<Throwable> suppressed = new ArrayList();

    private Throwable cause;
    
    public GroupIOException( Throwable cause ) {
        this.cause = cause;
        initCause( this.cause );
    }
    
    public void addSuppressed( Throwable t ) {
        suppressed.add( t );
    }

    public void printStackTrace( PrintStream out ) {

        cause.printStackTrace( out );

        for ( Throwable current : suppressed ) {
            current.printStackTrace( out );
        }
        
    }

    public void printStackTrace( PrintWriter out ) {

        cause.printStackTrace( out );

        for ( Throwable current : suppressed ) {
            current.printStackTrace( out );
        }

    }
    
}
    