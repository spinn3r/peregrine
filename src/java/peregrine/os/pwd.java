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
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

public class pwd {
    
    public static class Passwd extends Structure {

        public String name;
        public String passwd;
        public int uid;
        public int gid;
        public String gecos;
        public String dir;
        public String shell;
        
    }

    /**
     * The getpwnam() function returns a pointer to a structure containing the
     * broken-out fields of the record in the password database (e.g., the local
     * password file /etc/passwd, NIS, and LDAP) that matches the username name.
     * 
     * The getpwnam() and getpwuid() functions return a pointer to a passwd
     * structure, or NULL if the matching entry is not found or an error
     * occurs. If an error occurs, errno is set appropriately. If one wants to
     * check errno after the call, it should be set to zero before the call.
     * 
     */
    public static Passwd getpwnam( String name ) {
         return Delegate.getpwnam( name );
    }

    public static Passwd getpwuid( int uid ) {
         return Delegate.getpwuid( uid );
    }

    static class Delegate {
    
        public static native Passwd getpwnam( String name );
        public static native Passwd getpwuid( int uid );
        
        static {
            Native.register( "c" );
        }

    }

    public static void main( String[] args ) throws Exception {

        System.out.printf( "%s\n", pwd.getpwnam( "asdf" ) );
        
    }

}