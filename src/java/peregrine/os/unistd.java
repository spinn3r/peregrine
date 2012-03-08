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
import com.sun.jna.Pointer;

public class unistd {

    private static InterfaceDelegate delegate
        = (InterfaceDelegate)Native.loadLibrary( "c", InterfaceDelegate.class); 

    public static void sync() {
        delegate.sync();
    }

    public static int getpid() {
        return delegate.getpid();
    }

    /**
     * The setuid() function sets the real and effective user IDs and the saved
     * set-user-ID of the current process to the specified value.  The setuid()
     * function is permitted if the effective user ID is that of the super user,
     * or if the specified user ID is the same as the effective user ID.  If
     * not, but the specified user ID is the same as the real user ID, setuid()
     * will set the effective user ID to the real user ID.
     * 
     * The setgid() function sets the real and effective group IDs and the saved
     * set-group-ID of the current process to the specified value.  The setgid()
     * function is permitted if the effective user ID is that of the super user,
     * or if the specified group ID is the same as the effective group ID.  If
     * not, but the specified group ID is the same as the real group ID,
     * setgid() will set the effective group ID to the real group ID.
     * 
     * The seteuid() function (setegid()) sets the effective user ID (group ID)
     * of the current process.  The effective user ID may be set to the value of
     * the real user ID or the saved set-user-ID (see intro(2) and execve(2));
     * in this way, the effective user ID of a set-user-ID executable may be
     * toggled by switching to the real user ID, then re-enabled by reverting to
     * the set-user-ID value.  Simi- larly, the effective group ID may be set to
     * the value of the real group ID or the saved set-user-ID.
     * 
     * RETURN VALUES
     * 
     * Upon success, these functions return 0; otherwise -1 is returned.
     * 
     * If the user is not the super user, or the uid specified is not the real,
     * effective ID, or saved ID, these functions return -1.
     * 
     */
    public static int setuid( int uid ) throws Exception {

        int result = delegate.setuid( uid );

        if ( result == -1 )
            throw new PlatformException();
        
        return result;

    }


    /**
     * The getuid() function returns the real user ID of the calling process.
     * The geteuid() function returns the effective user ID of the calling
     * process.
     * 
     * The real user ID is that of the user who has invoked the program.  As the
     * effective user ID gives the process additional permissions during
     * execution of ``set-user-ID'' mode processes, getuid() is used to
     * determine the real-user-id of the calling process.
     * 
     * ERRORS
     * 
     * The getuid() and geteuid() functions are always successful, and no return
     * value is reserved to indicate an error.
     */
    public static int getuid() {	
    	return delegate.getuid();
    }
    
    interface InterfaceDelegate extends Library {
        void sync();
        int getpid();
        int setuid( int uid );
        int getuid();
    }

}
