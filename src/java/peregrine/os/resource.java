package peregrine.os;

import java.io.*;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Structure;
import com.sun.jna.ptr.*;

public class resource {

    private static InterfaceDelegate delegate
        = (InterfaceDelegate)Native.loadLibrary( "c", InterfaceDelegate.class); 

    public static final int RLIMIT_NOFILE         = 7;       /* max number of open files */

    public static class Rlimit extends Structure {
        
        public int rlim_cur = -1;  /* Soft limit */
        public int rlim_max = -1;  /* Hard limit (ceiling for rlim_cur) */

        @Override
        public String toString() {
            return String.format( "rlim_cur: %,d, , rlim_max: %,d" , rlim_cur, rlim_max );
        }
        
    }
    
    public static Rlimit getrlimit( int resource ) throws Exception {

        Rlimit result = new Rlimit();

        if ( delegate.getrlimit( resource, result ) != 0 ) {
            throw new Exception( errno.strerror() );
        }

        return result;
        
    }

    public static void setrlimit( int resource, Rlimit limit ) throws Exception {

        if ( delegate.setrlimit( resource, limit ) != 0 ) {
            throw new Exception( errno.strerror() );
        }
        
    }

    interface InterfaceDelegate extends Library {

        int getrlimit(int resource, Rlimit rlimit );
        int setrlimit(int resource, Rlimit rlimit );
        
    }
    
}
