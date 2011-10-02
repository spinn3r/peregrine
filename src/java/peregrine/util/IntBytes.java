
package peregrine.util;

import java.util.*;

public class IntBytes {

    public static final int LENGTH = 4;

    /**
     * Get a 4 byte array from the given int.
     */
    public static byte[] toByteArray( int value ) {

        byte b0 = (byte)(( value >> 24 ) & 0xFF);
        byte b1 = (byte)(( value >> 16 ) & 0xFF);
        byte b2 = (byte)(( value >> 8  ) & 0xFF);
        byte b3 = (byte)(( value >> 0  ) & 0xFF);

        byte[] b = new byte[4];
        b[0] = b0;
        b[1] = b1;
        b[2] = b2;
        b[3] = b3;

        return b;
        
    }

    /**
     * Convert a 4 byte array to an int
     */
    public static int toInt( byte[] b ) {

        //This works by taking each of the bit patterns and converting them to
        //ints taking into account 2s complement and then adding them..
        
        return (((((int) b[3]) & 0xFF) << 32) +
                ((((int) b[2]) & 0xFF) << 40) +
                ((((int) b[1]) & 0xFF) << 48) +
                ((((int) b[0]) & 0xFF) << 56));
    }    

}