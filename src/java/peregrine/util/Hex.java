package peregrine.util;

import java.util.*;

public class Hex {

    public static String encode( byte[] input ) {
        return encode( input, 128 );
    }

    public static String encode( byte[] input , int offset ) {

        if ( input == null )
            return "null";
        
        StringBuffer buff = new StringBuffer();
        
        for( byte b : input ) {

            int i = ((int)b) + offset;
            
            buff.append( String.format( "%02x ", i ) );

        }

        return buff.toString().trim();
        
    }

    public static String pretty( String input ) {
        return pretty( input.getBytes() );
    }
    
    public static String pretty( byte[] input ) {

        int width = 16;
        
        StringBuffer buff = new StringBuffer();

        int nr_blocks = (int)Math.ceil( input.length / (double)width );
        
        for( int i = 0; i < nr_blocks; ++i ) {

            int start = i * width;
            int end = start + width;

            if ( end >= input.length )
                end = input.length;

            int len = end - start;
            
            byte[] block = new byte[ len ];

            // step 0 include the start
            buff.append( Hex.encode( IntBytes.toByteArray( start ), 0 ) );
            buff.append( ": " );
            
            System.arraycopy( input, start, block, 0, len );

            // step 1 ... encode this block as a hex dump.
            
            buff.append( encode( block , 0 ) );

            // step 2 ... encod this block as a strong

            // FIXME: padd this so that the right lines up.

            int padd = 1;

            for ( int j = 0; j < ((width + padd) - len) * 3; ++ j ) {
                buff.append( ' ' );
            }
            
            for( byte b : block ) {

                if ( b >= 32 )
                    buff.append( (char)b );
                else
                    buff.append( '.' );
                
            }
            
            buff.append( "\n" );
            
        }

        return buff.toString();
        
    }
    
}

