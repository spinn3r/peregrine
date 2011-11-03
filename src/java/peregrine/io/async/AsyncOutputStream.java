
package peregrine.io.async;

/**
 * 
 */
public class AsyncOutputStream extends BaseAsyncOutputStream {
    
    public AsyncOutputStream( String path ) {

        AsyncOutputStreamCallable callable = new AsyncOutputStreamCallable( path, getQueue() );

        init( AsyncOutputStreamService.submit( callable ) );
        
    }

}
