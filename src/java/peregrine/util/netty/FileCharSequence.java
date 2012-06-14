package peregrine.util.netty;

import java.util.*;
import java.util.regex.*;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.*;

/**
 * Char seq backed by a file.
 */
public class FileCharSequence implements CharSequence {

    private int offset = 0;
    private RandomAccessFile in = null;
    private File file = null;

    private boolean trace = false;
    
    public FileCharSequence( String path ) throws IOException {
        this( new File( path ) );
    }

    public FileCharSequence( File file ) throws IOException {
        this( file , new RandomAccessFile( file, "r" ) );
    }

    public FileCharSequence( File file, RandomAccessFile in ) throws IOException {
        this.in = in;
        this.file = file;
    }

    public void setTrace( boolean trace ) {
        this.trace = trace;
    }
    
    public char charAt(int index) {

        if ( trace )
            System.out.printf( "charAt: %,d\n", index );
        
        try {

            //NOTE: this is safe but might NOT perform very well
            in.seek( index );
            
            char c = (char)in.read();
            ++offset;
            return c;

        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
        
    }

    public int length() {

        if ( trace )
            System.out.printf( "length()" );

        return (int)file.length();
    }
    
    public CharSequence subSequence( int start, int end ) {

        if ( trace )
            System.out.printf( "subSequence: %,d : %,d\n", start, end );

        char[] c = new char[ end - start ];

        int cidx=0;
        for( int i = start; i < end; ++i ) {
            c[cidx] = charAt( i );
            ++cidx;            
        }

        return new String( c );
        
    }

    public String toString() {

        if ( trace )
            System.out.printf( "toString" );

        throw new RuntimeException( "not implemented" );
    }
    
}
