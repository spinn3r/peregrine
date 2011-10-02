package peregrine.util;

import java.io.*;
import java.util.*;

public class VarintWriter {

    /**
     * Emit a varint to the output stream.  Only use with varint encoding.
     *
     */
    public byte[] write( int value ) {

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
    
}