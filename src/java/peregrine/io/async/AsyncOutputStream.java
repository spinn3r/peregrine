
package peregrine.io.async;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * 
 */
public class AsyncOutputStream extends BaseAsyncOutputStream {
    
    public AsyncOutputStream( String path ) {

        AsyncOutputStreamCallable callable = new AsyncOutputStreamCallable( path, getQueue() );

        init( AsyncOutputStreamService.submit( callable ) );
        
    }

}
