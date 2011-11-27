package peregrine.util;

import java.io.*;
import org.jboss.netty.buffer.*;

public class VarintReader {

    protected ChannelBuffer buff;
    
    public VarintReader() {}

    public VarintReader( ChannelBuffer buff ) {
        this.buff = buff;
    }
    
    public int read() {
        return read1() - 1;
    }

    private int read1() {

        byte tmp = buff.readByte();
        if (tmp >= 0) {
            return tmp;
        }
        int result = tmp & 0x7f;
        if ((tmp = buff.readByte()) >= 0) {
            result |= tmp << 7;
        } else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = buff.readByte()) >= 0) {
                result |= tmp << 14;
            } else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = buff.readByte()) >= 0) {
                    result |= tmp << 21;
                } else {
                    result |= (tmp & 0x7f) << 21;
                    result |= (tmp = buff.readByte()) << 28;
                    if (tmp < 0) {
                        // Discard upper 32 bits.
                        for (int i = 0; i < 5; i++) {
                            if (buff.readByte() >= 0) return result;
                        }
                        throw new RuntimeException( "Malformed varint." );
                    }
                }
            }
        }

        return result;
    }

}
