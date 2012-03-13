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
import com.sun.jna.Structure;

public class unistd {

    private static InterfaceDelegate delegate
        = (InterfaceDelegate)Native.loadLibrary( "c", InterfaceDelegate.class); 

    /**
     * The sync utility can be called to ensure that all disk writes have been
     * completed before the processor is halted in a way not suitably done by
     * shutdown(8).  Generally, it is preferable to use shutdown(8) to shut down
     * the system, as they may perform additional actions such as
     * resynchronizing the hardware clock and flushing internal caches before
     * performing a final sync.
     * 
     * The sync utility utilizes the sync(2) function call.
     */
    public static void sync() {
        delegate.sync();
    }

    /**
     * 
     * Getpid() returns the process ID of the calling process.  The ID is
     * guaranteed to be unique and is useful for constructing temporary file
     * names.
     * 
     * Getppid() returns the process ID of the parent of the calling process.
     * 
     * ERRORS
     * 
     * The getpid() and getppid() functions are always successful, and no return
     * value is reserved to indicate an error.
     * 
     */
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

    /**
     * The getpagesize() function returns the number of bytes in a page.  Page
     * granularity is the granularity of many of the memory management calls.
     * 
     * The page size is a system page size and may not be the same as the
     * underlying hardware page size.
     */
    public static int getpagesize() {
        return delegate.getpagesize();
    }

    /**
     * chown() changes the ownership of the file specified by path, which is
     * dereferenced if it is a symbolic link.
     * 
     */
    public static int chown( String path, int uid, int gid ) throws IOException {

        int result = delegate.chown( path, uid, gid );

        if ( result == -1 )
            throw new IOException( new PlatformException() );

        return result;
        
    }

    public static class StatStruct extends Structure {

        public long     st_dev;     /* ID of device containing file */
        public long     st_ino;     /* inode number */
        public long     st_nlink;   /* number of hard links */
        public int      st_mode;    /* protection */
        public int      st_uid;     /* user ID of owner */
        public int      st_gid;     /* group ID of owner */
        public long     st_rdev;    /* device ID (if special file) */
        public long     st_size;    /* total size, in bytes */
        public long     st_blksize; /* blocksize for filesystem I/O */
        public long     st_blocks;  /* number of blocks allocated */
        public long     st_atime;     // Time of last access (time_t)  
        public long     st_atimensec; // Time of last access (nanoseconds)  
        public long     st_mtime;     // Last data modification time (time_t)  
        public long     st_mtimensec; // Last data modification time (nanoseconds)  
        public long     st_ctime;     // Time of last status change (time_t)  
        public long     st_ctimensec; // Time of last status change (nanoseconds)  
        public long     __unused4;  
        public long     __unused5;  
        public long     __unused6;  
    }

    public static StatStruct stat( String path ) throws IOException {

        StatStruct stat = new StatStruct();

        int result = delegate.stat( path, stat );

        if ( result == -1 )
            throw new IOException( new PlatformException() );

        return stat;

    }

    interface InterfaceDelegate extends Library {

        void sync();
        int getpid();
        int setuid( int uid );
        int getuid();
        int getpagesize();
        int chown( String path, int uid, int gid );
        int stat( String path, StatStruct stat );

    }

}
