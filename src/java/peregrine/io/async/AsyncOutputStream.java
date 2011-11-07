
package peregrine.io.async;

import java.io.*;

/**
 * 
 */
public class AsyncOutputStream extends BaseAsyncOutputStream {

    public AsyncOutputStream( String path ) {
    	this( new File( path ) );
    }
	
    public AsyncOutputStream( File file ) {

        AsyncOutputStreamCallable callable = new AsyncOutputStreamCallable( file, getQueue() );

        init( AsyncOutputStreamService.submit( callable ) );
        
    }

}
