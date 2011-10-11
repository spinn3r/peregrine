package peregrine.util;

import java.io.*;
import java.util.*;

public class VarintReader {

    protected ByteReader byteReader;
    
    public VarintReader() {}
    
    public VarintReader( byte[] data, int index ) {
        this.byteReader = new ByteArrayByteReader( data, index );
    }

    public VarintReader( InputStream is ) {
        this.byteReader = new InputStreamByteReader( is );
    }

    public int read() {
        return read1() - 1;
    }
    
    public int read( byte[] data ) {
        return read( data, 0 );
    }

    public int read( byte[] data, int index ) {

        this.byteReader = new ByteArrayByteReader( data, index );

        //NOTE that all varints are incremented by one.  See emitVarint in the
        //encoder for more details.

        int result = read1() - 1;
        return result;
        
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

    private boolean isLastVarintByte( byte b ) {
        return (b >> 7) == 0;
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

        //FIXMEL this isn't right.
        
        try {
            return (byte)is.read();
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }

    }

}

class ByteArrayByteReader implements ByteReader {

    private int index = 0;

    private byte[] data;
    
    public ByteArrayByteReader( byte[] data, int index ) {
        this.data = data;
        this.index = index;
    }

    @Override
    public byte readByte() {

        byte b = data[index];

        ++index;
        return b;

    }

}