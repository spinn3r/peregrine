
package peregrine.util;

import java.util.*;
import java.lang.reflect.*;
import java.io.*;

import com.spinn3r.log5j.*;

public class Native {

    private static final Logger log = Logger.getLogger();

    /**
     * Used to get access to protected/private field of the specified class
     * @param klass - name of the class
     * @param fieldName - name of the field
     * @return Field or null on error
     */
    public static Field getProtectedField(Class klass, String fieldName) {

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
         
        Field field = getProtectedField(descriptor.getClass(), "fd");

        if ( field == null )
            return -1;

        try { 
            return field.getInt(descriptor);
        } catch (Exception e) {
            log.warn("unable to read fd field from FileDescriptor");
        }

        return -1;
        
    }

}