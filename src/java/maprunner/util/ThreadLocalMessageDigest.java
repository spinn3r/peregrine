package maprunner.util;

import java.security.*;

/**
 * ThreadLocal for the current MessageDigest.  Note that the SUN digest
 * factory is SLOW and this class will be 4x slower when using this
 * directly.  Also note that it's not threadsafe and will corrupt itself it
 * not threadlocal
 *
 * @author <a href="mailto:burton@tailrank.com">Kevin Burton</a>
 * @version $Id: ThreadLocalMessageDigest.java,v 1.2 2004/05/21 22:21:32 burton Exp $
 */
public class ThreadLocalMessageDigest extends ThreadLocal {

    private String name = null;
    
    public ThreadLocalMessageDigest( String name ) {
        this.name = name;
    }

    protected Object initialValue() {

        try {
            
            return MessageDigest.getInstance( name );
            
        } catch ( Exception e ) {

            e.printStackTrace();
            return null;

        }

    }
            
}
