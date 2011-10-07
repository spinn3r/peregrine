
package peregrine.io.async;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * 
 */
public class AsyncOutputStream extends BaseAscyncOutputStream {
    
    public AsyncOutputStream( String dest ) {

        AsyncOutputStreamCallable callable = new AsyncOutputStreamCallable( dest, getQueue() );

        init( AsyncOutputStreamService.submit( callable ) );
        
    }

}
