package peregrine.util;

import java.io.*;

import peregrine.util.netty.*;

import org.jboss.netty.buffer.*;

public class VarintReader {

    protected StreamReader reader;
    
    public VarintReader( ChannelBuffer buff ) {
        this( new StreamReader( buff ) );
    }

    public VarintReader( StreamReader reader ) {
        this.reader = reader;
    }
    
    public int read() {
        return read1() - 1;
    }

    private int read1() {

        byte tmp = reader.read();
        if (tmp >= 0) {
            return tmp;
        }
        int result = tmp & 0x7f;
        if ((tmp = reader.read()) >= 0) {
            result |= tmp << 7;
        } else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = reader.read()) >= 0) {
                result |= tmp << 14;
            } else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = reader.read()) >= 0) {
                    result |= tmp << 21;
                } else {
                    result |= (tmp & 0x7f) << 21;
                    result |= (tmp = reader.read()) << 28;
                    if (tmp < 0) {
                        // Discard upper 32 bits.
                        for (int i = 0; i < 5; i++) {
                            if (reader.read() >= 0) return result;
                        }
                        throw new RuntimeException( "Malformed varint." );
                    }
                }
            }
        }

        return result;
    }

}
