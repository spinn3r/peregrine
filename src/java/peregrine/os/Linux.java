package peregrine.os;

import java.io.*;

public class Linux {

    public static void dropCaches() throws IOException {

        FileOutputStream fos = new FileOutputStream( "/proc/sys/vm/drop_caches" );
        fos.write( (byte)'3' );
        fos.close();

    }

}