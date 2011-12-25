package peregrine.io.util;

import java.util.*;
import java.io.*;

/**
 * Implements JDK 1.7 try-with-resources style closing for multiple Closeables /
 * Flushables with an exception that includes all exceptions.
 *
 * @see <a href='http://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html'>try-with-resources</a>
 */
public class GroupIOException extends IOException {

    List<Throwable> suppressed = new ArrayList();
    
    public GroupIOException( Throwable cause ) {
        super( cause );
    }
    
    public void addSuppressed( Throwable t ) {
        suppressed.add( t );
    }

    public void printStackTrace( PrintStream out ) {

        // this will print ourselves AND the cause... 
        printStackTrace( out );

        for ( Throwable current : suppressed ) {
            current.printStackTrace( out );
        }
        
    }

    public void printStackTrace( PrintWriter out ) {

        // this will print ourselves AND the cause... 
        printStackTrace( out );

        for ( Throwable current : suppressed ) {
            current.printStackTrace( out );
        }

    }
    
}
    