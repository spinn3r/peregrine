
package peregrine.sysstat;

import java.io.*;
import java.util.*;
import java.math.*;

/**
 */
public interface Diffable<T> {

    public T diff( T after );

    public T rate( long interval );
    
}