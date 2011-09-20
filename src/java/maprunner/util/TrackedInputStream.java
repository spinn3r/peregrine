package maprunner.util;

import java.io.*;

public class TrackedInputStream extends BufferedInputStream {

    private int position = 0;

    public TrackedInputStream( InputStream is ) {
        super( is );
    }
    
    public int read() throws IOException {

        ++position;
        return super.read();
    }

    public int read( byte[] b,
                     int off,
                     int len )
        throws IOException {

        position += len;
        return super.read( b, off, len );

    }
    
    public int read(byte[] b) throws IOException {
        position += b.length;
        return super.read( b );
    }

    public int getPosition() {
        return position;
    }

}