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
import java.lang.reflect.*;

import com.spinn3r.log5j.Logger;

/**
 */
public class Platform {

    private static final Logger log = Logger.getLogger();

    public static String getOS() {
        return System.getProperty("os.name").toLowerCase();
    }
    
    public static boolean isLinux() {
        return getOS().contains("linux");
    }

    public static boolean isDarwin() {
        return getOS().contains("mac os x");
    }

    /**
     * Used to get access to protected/private field of the specified class
     * @param klass - name of the class
     * @param fieldName - name of the field
     * @return Field or null on error
     */
    private static Field getProtectedField(Class klass, String fieldName) {

        Field field;

        try {
            field = klass.getDeclaredField(fieldName);
            field.setAccessible(true);
        } catch (Exception e) {
            throw new AssertionError(e);
        }

        return field;
    }

     public static int getFd( FileDescriptor descriptor ) {
         
        Field field = getProtectedField( descriptor.getClass(), "fd" );

        try { 
            return field.getInt(descriptor);
        } catch (Exception e) {
            log.warn("unable to read fd field from FileDescriptor");
        }

        return -1;
        
    }

}
