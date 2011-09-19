package maprunner.util;

import java.io.*;
import java.util.*;

public class VarintReader {

    byte[] data = null;
    int index = -1;

    public VarintReader() {}
    
    public VarintReader( byte[] data, int index ) {
        this.data = data;
        this.index = index;
    }

    public int read() {
        //NOTE: we have to make sure the user uses the right constructor.
        return read( this.data, this.index );
    }

    public int read( byte[] data ) {
        return read( data, 0 );
    }

    public int read( byte[] data, int index ) {

        this.data = data;
        this.index = index;
        
        //NOTE that all varints are incremented by one.  See emitVarint in the
        //encoder for more details.

        int result = read1() - 1;
        return result;
    }

    private int read1() {

        byte tmp = readByte();
        if (tmp >= 0) {
            return tmp;
        }
        int result = tmp & 0x7f;
        if ((tmp = readByte()) >= 0) {
            result |= tmp << 7;
        } else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = readByte()) >= 0) {
                result |= tmp << 14;
            } else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = readByte()) >= 0) {
                    result |= tmp << 21;
                } else {
                    result |= (tmp & 0x7f) << 21;
                    result |= (tmp = readByte()) << 28;
                    if (tmp < 0) {
                        // Discard upper 32 bits.
                        for (int i = 0; i < 5; i++) {
                            if (readByte() >= 0) return result;
                        }
                        throw new RuntimeException( "Malformed varint." );
                    }
                }
            }
        }

        return result;
    }

    private byte readByte() {
        byte b = data[index];

        ++index;
        return b;
        
    }

    public int getIndex() {
        return index;
    }
    
}
