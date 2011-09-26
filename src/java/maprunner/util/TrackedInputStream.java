package maprunner.util;

import java.io.*;

public class TrackedInputStream {

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
    
}