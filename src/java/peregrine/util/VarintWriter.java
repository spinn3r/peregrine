package peregrine.util;

import java.util.*;

import org.jboss.netty.buffer.*;

public class VarintWriter {

    /**
     * Emit a varint to the output stream.  Only use with varint encoding.
     */
    public static void write( ChannelBuffer buff, int value ) {

        //note varints have to be incremented by one(1) so that we can avoid
        //having to store a zero offset.  If we were to do this then it would be
        //0x00 0x00 which is an escaped 0x00 and would be inserted into the
        //stream as a literal.

        ++value;

        while (true) {
            if ((value & ~0x7F) == 0) {
                buff.writeByte( (byte)value );
                break;
            } else {
                buff.writeByte((byte)((value & 0x7F) | 0x80));
                value >>>= 7;
            }
        }

    }

    /**
     * Given an int, return the number of bytes required for storage.
     */
    public static int sizeof( int value ) {

        if ( value <= 126 ) 
            return 1;
        else if ( value <= 16382 ) /* huh? 2^14 */ 
            return 2;
        else if ( value <= 2097150 )
            return 3;
        return 4;

    }
    
}