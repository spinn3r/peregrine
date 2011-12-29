/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.os;

import java.io.*;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Structure;
import com.sun.jna.ptr.*;

public class resource {

    private static InterfaceDelegate delegate
        = (InterfaceDelegate)Native.loadLibrary( "c", InterfaceDelegate.class); 

    /**
     * FIXME: This works but on OS X this is defined as 8 and on Linux it's 7 ... WTF
     * that isn't helpful.
     */
    public static final int RLIMIT_NOFILE = 7; /* max number of open files */

    public static class Rlimit extends Structure {

        public Rlimit() {};
        
        public Rlimit( long value ) {
            rlim_cur = value;
            rlim_max = value;
        }
        
        public long rlim_cur = -1;  /* Soft limit */
        public long rlim_max = -1;  /* Hard limit (ceiling for rlim_cur) */

        @Override
        public String toString() {
            return String.format( "rlim_cur: %,d, , rlim_max: %,d" , rlim_cur, rlim_max );
        }
        
    }
    
    public static Rlimit getrlimit( int resource ) throws Exception {

        assertLinux();

        Rlimit result = new Rlimit();

        if ( delegate.getrlimit( resource, result ) != 0 ) {
            throw new Exception( errno.strerror() );
        }

        return result;
        
    }

    public static void setrlimit( int resource, Rlimit limit ) throws Exception {

        assertLinux();
        
        if ( delegate.setrlimit( resource, limit ) != 0 ) {
            throw new Exception( errno.strerror() );
        }
        
    }

    private static void assertLinux() {
        
        if ( Platform.isLinux() == false )
            throw new RuntimeException( "Platform is not linux" );
        
    }
    
    interface InterfaceDelegate extends Library {

        int getrlimit(int resource, Rlimit rlimit );
        int setrlimit(int resource, Rlimit rlimit );
        
    }

    public static void main( String[] args ) throws Exception {

        System.out.printf( "getrlimit(RLIMIT_NOFILE): %s\n", resource.getrlimit( resource.RLIMIT_NOFILE ) );

    }
    
}
