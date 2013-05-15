/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

    /*

      The following code will print out these constants on Linux:
      
        #include <sys/time.h>
        #include <sys/resource.h>
        #include <stdio.h>

        int main() {
            printf( "RLIMIT_AS: %d\n",       RLIMIT_AS );
            printf( "RLIMIT_MEMLOCK: %d\n",  RLIMIT_MEMLOCK );
            printf( "RLIMIT_RSS: %d\n",      RLIMIT_RSS );
            printf( "RLIMIT_NOFILE: %d\n",   RLIMIT_NOFILE );
        }

      On Linux these values are:

        RLIMIT_AS: 9
        RLIMIT_MEMLOCK: 8
        RLIMIT_RSS: 5
        RLIMIT_NOFILE: 7

      But on OS X these values are:

        RLIMIT_AS: 5
        RLIMIT_MEMLOCK: 6
        RLIMIT_RSS: 5
        RLIMIT_NOFILE: 8

      TODO: find some way we can have this work on ALL platforms without having
      to change the constants.
        
    */
    public enum RLIMIT {

        /**
         * The maximum size of the process's virtual memory (address space) in
         * bytes. This limit affects calls to brk(2), mmap(2) and mremap(2), which
         * fail with the error ENOMEM upon exceeding this limit. Also automatic
         * stack expansion will fail (and generate a SIGSEGV that kills the process
         * if no alternate stack has been made available via sigaltstack(2)). Since
         * the value is a long, on machines with a 32-bit long either this limit is
         * at most 2 GiB, or this resource is unlimited.
         */
        AS(9,5)       ,

        /**
         * The maximum number of bytes of memory that may be locked into RAM. In
         * effect this limit is rounded down to the nearest multiple of the system
         * page size. This limit affects mlock(2) and mlockall(2) and the mmap(2)
         * MAP_LOCKED operation. Since Linux 2.6.9 it also affects the shmctl(2)
         * SHM_LOCK operation, where it sets a maximum on the total bytes in shared
         * memory segments (see shmget(2)) that may be locked by the real user ID of
         * the calling process. The shmctl(2) SHM_LOCK locks are accounted for
         * separately from the per-process memory locks established by mlock(2),
         * mlockall(2), and mmap(2) MAP_LOCKED; a process can lock bytes up to this
         * limit in each of these two categories. In Linux kernels before 2.6.9,
         * this limit controlled the amount of memory that could be locked by a
         * privileged process. Since Linux 2.6.9, no limits are placed on the amount
         * of memory that a privileged process may lock, and this limit instead
         * governs the amount of memory that an unprivileged process may lock.
         */
        MEMLOCK(8,6)  ,

        /**
         * Specifies a value one greater than the maximum file descriptor number
         * that can be opened by this process. Attempts (open(2), pipe(2), dup(2),
         * etc.) to exceed this limit yield the error EMFILE. (Historically, this
         * limit was named RLIMIT_OFILE on BSD.)
         */
        NOFILE(7,8)   ;

        private final int value;
            
        RLIMIT( int value_linux, int value_darwin ) {

            if ( Platform.isLinux() ) {
                value = value_linux;
            } else if ( Platform.isDarwin() ) {
                value = value_darwin;
            } else {
                throw new RuntimeException( "Unknown platform: " + Platform.getOS() );
            }
            
        }

        public int value() {
            return value;
        }
        
    }
    
    public static class RlimitStruct extends Structure {

        public RlimitStruct() {};
        
        public RlimitStruct( long value ) {
            rlim_cur = value;
            rlim_max = value;
        }

        /**
         * Soft limit.
         */
        public long rlim_cur = -1;  /* Soft limit */

        /**
         * Hard limit (ceiling for rlim_cur).
         */
        public long rlim_max = -1;  /* Hard limit (ceiling for rlim_cur) */

        @Override
        public String toString() {
            return String.format( "rlim_cur: %,d, , rlim_max: %,d" , rlim_cur, rlim_max );
        }
        
    }
    
    public static RlimitStruct getrlimit( RLIMIT resource ) throws Exception {

        assertPlatform();

        RlimitStruct result = new RlimitStruct();

        if ( delegate.getrlimit( resource.value(), result ) != 0 ) {
            throw new Exception( errno.strerror() );
        }

        return result;
        
    }

    public static void setrlimit( RLIMIT resource, RlimitStruct limit ) throws Exception {

        assertPlatform();
        
        if ( delegate.setrlimit( resource.value(), limit ) != 0 ) {
            throw new PlatformException( "Unable to setrlimit on: " + resource );
        }

        // now make sure we actually wrote the correct resources.

        RlimitStruct after = getrlimit( resource );

        if ( after.rlim_cur != limit.rlim_cur ||
             after.rlim_max != limit.rlim_max ) {

            throw new Exception( String.format( "Limit set incorrectly (requested %s vs %s) on " + resource ,
                                                limit, after ) );
            
        }
        
    }

    private static void assertPlatform() {
        
        if ( Platform.isLinux() || Platform.isDarwin() ) {
            return;
        }
            
        throw new RuntimeException( "Platform is not supported: " + Platform.getOS() );
        
    }
    
    interface InterfaceDelegate extends Library {

        int getrlimit(int resource, RlimitStruct rlimit );
        int setrlimit(int resource, RlimitStruct rlimit );
        
    }

    public static void main( String[] args ) throws Exception {

        //System.out.printf( "getrlimit(RLIMIT_NOFILE): %s\n", resource.getrlimit( resource.RLIMIT_NOFILE ) );

    }
    
}
