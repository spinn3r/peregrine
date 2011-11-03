package peregrine.util;

import java.io.*;
import org.jboss.netty.buffer.*;

public class VarintReader {

    protected ByteReader byteReader;
    
    public VarintReader() {}

    public VarintReader( InputStream is ) {
        this.byteReader = new InputStreamByteReader( is );
    }

    public VarintReader( ChannelBuffer buff ) {
        this( new ChannelBufferInputStream( buff ) );
    }
    
    public int read() {
        return read1() - 1;
    }

    private int read1() {

        byte tmp = byteReader.readByte();
        if (tmp >= 0) {
            return tmp;
        }
        int result = tmp & 0x7f;
        if ((tmp = byteReader.readByte()) >= 0) {
            result |= tmp << 7;
        } else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = byteReader.readByte()) >= 0) {
                result |= tmp << 14;
            } else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = byteReader.readByte()) >= 0) {
                    result |= tmp << 21;
                } else {
                    result |= (tmp & 0x7f) << 21;
                    result |= (tmp = byteReader.readByte()) << 28;
                    if (tmp < 0) {
                        // Discard upper 32 bits.
                        for (int i = 0; i < 5; i++) {
                            if (byteReader.readByte() >= 0) return result;
                        }
                        throw new RuntimeException( "Malformed varint." );
                    }
                }
            }
        }

        return result;
    }

}

interface ByteReader {

    public byte readByte();

}

class InputStreamByteReader implements ByteReader {

    private InputStream is;
    
    public InputStreamByteReader( InputStream is ) {
        this.is = is;
    }
    
    @Override
    public byte readByte() {

        //FIXME this isn't right.  We shouldnt' be throwing a RuntimeException
        //here... 
        
        try {
            return (byte)is.read();
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }

    }

}
