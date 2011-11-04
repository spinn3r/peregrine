package peregrine.util;

import java.util.*;

import org.jboss.netty.buffer.*;

public class VarintWriter {

    // FIXME: Struct.java is the ONLY on using this and it will need to go away.

    /**
     * Emit a varint to the output stream.  Only use with varint encoding.
     */
    public static byte[] write( int value ) {

        List<Byte> list = new ArrayList();
        
        //note varints have to be incremented by one(1) so that we can avoid
        //having to store a zero offset.  If we were to do this then it would be
        //0x00 0x00 which is an escaped 0x00 and would be inserted into the
        //stream as a literal.

        ++value;
        
        while (true) {
            if ((value & ~0x7F) == 0) {
                list.add( (byte)value );
                break;
            } else {
                list.add((byte)((value & 0x7F) | 0x80));
                value >>>= 7;
            }
        }

        //NOTE this isn't amazingly efficient but it allows us to unit test.  It
        //MIGHT make sense to create a byte[] with the length of five(5) and
        //then System.arraycopy it into a smaller byte array.
        byte[] result = new byte[ list.size() ];
        for( int i = 0; i < result.length; ++i ) {
            result[i] = list.get( i );
        }

        return result;
        
    }

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