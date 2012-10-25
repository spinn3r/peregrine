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
import java.nio.*;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import peregrine.*;
import peregrine.util.*;

public class unistd {

    private static InterfaceDelegate delegate
        = (InterfaceDelegate)Native.loadLibrary( "c", InterfaceDelegate.class); 

    public static final int S_IXOTH = 1 ;
    public static final int S_IWOTH = 2 ;
    public static final int S_IROTH = 4 ;

    public static final int S_IRWXO = S_IROTH | S_IWOTH | S_IXOTH;

    public static final int S_IXGRP = 8  ;
    public static final int S_IWGRP = 16 ;
    public static final int S_IRGRP = 32 ;

    public static final int S_IRWXG = S_IRGRP | S_IWGRP | S_IXGRP;

    public static final int S_IXUSR = 64  ;
    public static final int S_IWUSR = 128 ;
    public static final int S_IRUSR = 256 ;

    public static final int S_IRWXU = S_IRUSR | S_IWUSR | S_IXUSR;

    public static final int ACCESSPERMS = S_IRWXU|S_IRWXG|S_IRWXO;

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
            throw new IOException( "Unable to chown: " + path, new PlatformException() );

        return result;
        
    }

    public static int chmod( String path, int mode ) throws IOException {

        int result = delegate.chmod( path, mode );

        if ( result == -1 )
            throw new IOException( "Unable to chmod: " + path, new PlatformException() );

        return result;
        
    }

    public static int mkdir( String path, int mode ) throws IOException {

        int result = delegate.mkdir( path, mode );

        if ( result == -1 )
            throw new IOException( "Unable to mkdir: " + path, new PlatformException() );

        return result;

    }

    /**
     * unlink() deletes a name from the file system. If that name was the last
     * link to a file and no processes have the file open the file is deleted
     * and the space it was using is made available for reuse.
     * 
     * If the name was the last link to a file but any processes still have the
     * file open the file will remain in existence until the last file
     * descriptor referring to it is closed.
     * 
     * If the name referred to a symbolic link the link is removed.
     * 
     * If the name referred to a socket, fifo or device the name for it is
     * removed but processes which have the object open may continue to use it.
     * 
     * Return Value
     * 
     * On success, zero is returned. On error, -1 is returned, and errno is set
     * appropriately.
     * 
     */
    public static int unlink( String path ) throws IOException {

        int result = delegate.unlink( path );

        if ( result == -1 )
            throw new IOException( "Unable to unlink: " + path , new PlatformException() );

        return result;

    }

    /**
     * rename() renames a file, moving it between directories if required. Any
     * other hard links to the file (as created using link(2)) are
     * unaffected. Open file descriptors for oldpath are also unaffected.
     * 
     * If newpath already exists it will be atomically replaced (subject to a
     * few conditions; see ERRORS below), so that there is no point at which
     * another process attempting to access newpath will find it missing.
     * 
     * If oldpath and newpath are existing hard links referring to the same
     * file, then rename() does nothing, and returns a success status.
     * 
     * If newpath exists but the operation fails for some reason rename()
     * guarantees to leave an instance of newpath in place.
     * 
     * oldpath can specify a directory. In this case, newpath must either not
     * exist, or it must specify an empty directory.
     * 
     * However, when overwriting there will probably be a window in which both
     * oldpath and newpath refer to the file being renamed.
     * 
     * If oldpath refers to a symbolic link the link is renamed; if newpath
     * refers to a symbolic link the link will be overwritten.
     */

    public static int rename( String oldPath, String newPath ) throws IOException {

        int result = delegate.rename( oldPath, newPath );

        if ( result == -1 )
            throw new IOException( String.format( "Unable to rename %s to %s" , oldPath , newPath ) ,
                                   new PlatformException() );

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

    public static int fork() throws Exception {

        int result = delegate.fork();

        if ( result == -1 )
            throw new PlatformException();

        return result;

    }

    public static int execve( String path, String[] argv, String [] envp ) throws Exception {

        int result = delegate.execve( path, argv, envp );

        if ( result == -1 )
            throw new PlatformException();

        return result;
        
    }

    /**
     * Simple implementation of spawn which combines fork/exec into the same
     * call under the covers in glibc land.
     */
    public static int posix_spawn( String path,
                                   String[] argv, 
                                   String[] envp ) {

        Pointer NULL = new Pointer( 0 );
        
        return delegate.posix_spawn( 0, path, NULL, NULL, argv, envp );
        
    }

    public static int posix_spawn( int pid,
                                   String path,
                                   Pointer fileActions, 
                                   Pointer attr,
                                   String[] argv, 
                                   String[] envp ) {

        return delegate.posix_spawn( pid, path, fileActions, attr, argv, envp );
        
    }
    
    interface InterfaceDelegate extends Library {

        void sync();
        int getpid();
        int setuid( int uid );
        int getuid();
        int getpagesize();
        int chown( String path, int uid, int gid );
        int chmod( String path, int perms );
        int stat( String path, StatStruct stat );
        int mkdir( String path, int mode );
        int unlink( String path );
        int rename( String oldPath, String newPath );
        int fork();
        int execve( String path, String[] argv, String [] envp );

        int posix_spawn( int pid,
                         String path,
                         Pointer fileActions, 
                         Pointer attr,
                         String[] argv, 
                         String[] envp );
        
    }

}
