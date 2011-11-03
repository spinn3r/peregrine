package peregrine.util;

import org.jboss.netty.buffer.*;

public class Hex {

    public static String encode( byte[] input ) {

        if ( input == null )
            return null;

        return encode( ChannelBuffers.wrappedBuffer( input ) );
        
    }

    public static String encode( ChannelBuffer buff ) {

        StringBuffer result = new StringBuffer();

        String hex = ChannelBuffers.hexDump( buff );

        for (int i = 0; i < hex.length(); ++i ) {

            result.append( hex.charAt( i ) );
            
            if ( ( i + 1 ) % 2 == 0 )
                result.append( ' ' );
            
        }
        
        return result.toString();
        
    }
    
    public static String pretty( String input ) {

        if ( input == null )
            return null;

        return pretty( input.getBytes() );
    }

    public static String pretty( byte[] input ) {

        if ( input == null )
            return null;

        return pretty( ChannelBuffers.wrappedBuffer( input ) );

    }
    
    public static String pretty( ChannelBuffer buff ) {

        int width = 16;
        
        StringBuffer result = new StringBuffer();

        int nr_blocks = (int)Math.ceil( buff.capacity() / (double)width );

        for( int i = 0; i < nr_blocks; ++i ) {

            int start = i * width;
            int end = start + width;

            if ( end >= buff.capacity() )
                end = buff.capacity();

            int len = end - start;

            ChannelBuffer index = ChannelBuffers.buffer( IntBytes.LENGTH );
            index.writeInt( start );
            
            // step 0 include the start
            result.append( encode( index ) );
            result.append( ": " );

            ChannelBuffer slice = buff.slice( start, len );
            
            // step 1 ... encode this block as a hex dump.
            
            result.append( encode( slice ) );

            // step 2 ... encod this block as a strong

            int padd = 1;

            for ( int j = 0; j < ((width + padd) - len) * 3; ++ j ) {
                result.append( ' ' );
            }
            
            for( int j = 0; j < slice.capacity(); ++j ) {

                byte b = slice.getByte( j );
                
                if ( b >= 32 )
                    result.append( (char)b );
                else
                    result.append( '.' );
                
            }
            
            result.append( "\n" );
            
        }

        return result.toString();
        
    }
    
}

