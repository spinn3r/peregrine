
package peregrine.util;

import java.util.*;

public class DoubleBytes {

    public static final int LENGTH = 8;

    /**
     */
    public static byte[] toByteArray( double value ) {
        return LongBytes.toByteArray( Double.doubleToLongBits( value ) );
    }

}