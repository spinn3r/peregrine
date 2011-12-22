package peregrine.os;

import java.io.*;

import com.spinn3r.log5j.Logger;

public class Linux {

    private static final Logger log = Logger.getLogger();

    public static void dropCaches() throws IOException {

        String os = System.getProperty("os.name").toLowerCase();

        // only attempt to run this on Linux.
        if ( ! os.contains("linux") ) {
            return;
        }

        log.info( "Dropping caches." );
        
        FileOutputStream fos = new FileOutputStream( "/proc/sys/vm/drop_caches" );
        fos.write( (byte)'3' );
        fos.close();

    }

}