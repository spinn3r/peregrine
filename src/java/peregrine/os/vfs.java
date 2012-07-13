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

public class vfs {

    public static class StatfsStruct extends Structure {
        
        // statfs("/boot", {f_type="EXT2_SUPER_MAGIC", f_bsize=1024, f_blocks=233191, f_bfree=193652, f_bavail=181211, f_files=124496, f_ffree=124265, f_fsid={1023140949, 166541809}, f_namelen=255, f_frsize=1024}) = 0

        public long f_type;     /* type of file system (see below) */
        public long f_bsize;    /* optimal transfer block size */
        public long f_blocks;   /* total data blocks in file system */
        public long f_bfree;    /* free blocks in fs */
        public long f_bavail;   /* free blocks available to unprivileged user */
        public long f_files;    /* total file nodes in file system */
        public long f_ffree;    /* free file nodes in fs */
        public int[] f_fsid = new int[2]; /* file system id */ /* struct { int val[2]; }. */
        public long f_namelen; /* maximum length of filenames */
        public long f_frsize; /* fragment size (since Linux 2.6) */
        public long[] f_spare = new long[5];

        @Override
        public String toString() {
            return String.format( "f_type: %s, f_bsize: %s, f_blocks: %s, f_bfree: %s, f_bavail: %s" , f_type, f_bsize, f_blocks, f_bfree, f_bavail );
        }
        
    }

    public static class FsidStruct {
        public int[] val = new int[2];
    }

    public static void statfs( String path, StatfsStruct struct )
        throws IOException {

        if ( Delegate.statfs( path, struct ) != 0 ) {
            PlatformException cause = new PlatformException();
            throw new IOException( String.format( "Unable to statfs: %s" , path, cause.getMessage() ), cause );
        }

    }
    
    static class Delegate {

        public static native int statfs( String path, StatfsStruct struct );

        static {
            Native.register( "c" );
        }

    }

    public static void main( String[] args ) throws Exception {

        StatfsStruct struct = new StatfsStruct();
        statfs( "/", struct );

        System.out.printf( "struct: %s\n", struct );

    }
    
}
