package peregrine.util;

import java.util.*;

import org.jboss.netty.buffer.*;

public class Hex {

    public static String encode( byte[] input ) {
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
        return pretty( input.getBytes() );
    }
    
    public static String pretty( byte[] input ) {

        int width = 16;
        
        StringBuffer result = new StringBuffer();

        int nr_blocks = (int)Math.ceil( input.length / (double)width );

        ChannelBuffer buff = ChannelBuffers.wrappedBuffer( input );
        
        for( int i = 0; i < nr_blocks; ++i ) {

            int start = i * width;
            int end = start + width;

            if ( end >= input.length )
                end = input.length;

            int len = end - start;
            
            byte[] block = new byte[ len ];

            ChannelBuffer index = ChannelBuffers.buffer( 4 );
            index.writeInt( start );
            
            // step 0 include the start
            result.append( encode( index ) );
            result.append( ": " );
            
            System.arraycopy( input, start, block, 0, len );

            ChannelBuffer slice = buff.slice( start, len );
            
            // step 1 ... encode this block as a hex dump.
            
            result.append( encode( slice ) );

            // step 2 ... encod this block as a strong

            int padd = 1;

            for ( int j = 0; j < ((width + padd) - len) * 3; ++ j ) {
                result.append( ' ' );
            }
            
            for( byte b : block ) {

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

