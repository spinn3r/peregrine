package peregrine.values;

import java.io.*;
import java.util.*;

import org.jboss.netty.buffer.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.util.primitive.*;

/**
 * Stores a list of byte arrays.  The nice thing is that you can just APPEND to
 * this list when you want to add entries to it...
 */
public class Struct {

    public List<StructReader> values = new ArrayList();

    public Struct() {}

    public Struct( ChannelBuffer data ) {
        fromChannelBuffer( data );
    }

    //WRITERS

    public Struct write( List<StructReader> values ) {

        for( StructReader val : values ) {
            this.values.add( val );
        }

        return this;

    }

    public Struct write( StructReader value ) {
        values.add( value );
        return this;
    }

    // READERS

    public List<StructReader> read() {
        return values;
    }

    public ChannelBuffer toChannelBuffer() {

    	int len = 0;
        for( StructReader value : values ) {
            len += IntBytes.LENGTH + value.length();
        }

        ChannelBuffer buff = ChannelBuffers.buffer( len );

        //TODO: support fixed with encoding if ALL are the same width
        for( StructReader value : values ) {
            VarintWriter.write( buff, value.length() );
            buff.writeBytes( value.getChannelBuffer() );
        }
        
        return buff;

    }

    public void fromChannelBuffer( ChannelBuffer buff ) {

        VarintReader varintReader = new VarintReader( buff );
        
        while( buff.readerIndex() < buff.writerIndex() ) {

            int len = varintReader.read();
            values.add( new StructReader( buff.readSlice( len ) ) );
        }

    }

    @Override
    public String toString() {

        StringBuffer buff = new StringBuffer();
        
        for ( StructReader val : values ) {

            buff.append( String.format( "%s ", Hex.encode( val.getChannelBuffer() ) ) );
            
        }

        return buff.toString();
        
    }
    
}