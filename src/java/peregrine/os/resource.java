package peregrine.os;

import java.io.*;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Structure;

public class resource {

    private static InterfaceDelegate delegate
        = (InterfaceDelegate)Native.loadLibrary( "c", InterfaceDelegate.class); 

    public static class Rlimit {
        
        public int rlim_cur;  /* Soft limit */
        public int rlim_max;  /* Hard limit (ceiling for rlim_cur) */
        
    }
    
    public static Rlimit getrlimit( int resource ) throws Exception {

        Rlimit result = new Rlimit();

        if ( delegate.getrlimit( resource, result ) != 0 ) {
            throw new Exception( errno.strerror() );
        }

        return result;
        
    }
    
    interface InterfaceDelegate extends Library {

        int getrlimit(int resource, Rlimit rlimit );
        
    }
    
}
