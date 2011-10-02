package peregrine.util;

import java.io.*;

public class TrackedInputStream extends InputStream {

    private InputStream delegate = null;
    
    private int position = 0;

    public TrackedInputStream( InputStream is ) {
        delegate = new BufferedInputStream( is );
    }

    public int read() throws IOException {

        ++position;
        return delegate.read();
    }

    public int read( byte[] b,
                     int off,
                     int len )
        throws IOException {

        position += len;
        return delegate.read( b, off, len );

    }
    
    public int read( byte[] b ) throws IOException {
        position += b.length;
        return delegate.read( b );
    }

    public int getPosition() {
        return position;
    }

    public void close() throws IOException {
        delegate.close();
    }

    public int available() throws IOException {
        return delegate.available();
    }

    public void mark( int readlimit ) {
        delegate.mark( readlimit );
    }

    public void reset() throws IOException {
        delegate.reset();
    }

}