/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.console.fs;

import peregrine.*;
import peregrine.util.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.util.*;

import java.io.*;
import java.util.*;

/**
 * Cat a file on the filesytem.  This reads from a local PFS directory file
 * (someday this may be globally via HTTP across the entire filesystem) and cats
 * the file to standard out.
 * 
 * <p>
 * 
 * This can be used for debug purposes so that one can understand what is stored
 * on the filesystem without having to write additional map reduce jobs.
 */
public class Cat {

    public static void help() {

        System.out.printf( "Usage: fscat [OPTION] [FILE]\n" );
        System.err.printf( "Simple Peregrine filesystem 'cat' command \n" );
        System.err.printf( "\n" );
        System.out.printf( "  --render   Render the value(s) in the following format.\n" );
        System.out.printf( "             Multiple columns are separated by a comma (,)\n" );
        System.err.printf( "\n" );
        System.out.printf( "  --limit    Limit results to the first N entries.\n" );        
        System.out.printf( "\n" );
        System.out.printf( "The --render option supports a comma separated list\n" );
        System.out.printf( "of values to print from the byte stream.\n" );
        System.out.printf( "\n" );
        
    }
    
    public static void main( String[] args ) throws Exception {

        Getopt getopt = new Getopt( args );

        if ( getopt.getBoolean( "help" ) ) {
            help();
            System.exit( 1 );
        }
        
        String render  = getopt.getString( "render" , "base64" );
        int limit      = getopt.getInt( "limit", -1 );

        //TODO: go over ALL paths specified so the user can cat multiple files.
        String path = getopt.getValues().get( 0 );

        SequenceReader reader = null;

        try {
            
            reader = new DefaultChunkReader( new File( path ) );

            int id = 0;
            while ( reader.hasNext() ) {
                
                reader.next();
                
                StructReader key   = reader.key();
                StructReader value = reader.value();
                
                System.out.printf( "%s = %s\n", Base64.encode( key.toByteArray() ), format( render, value ) );

                if ( limit != -1 && ++id >= limit )
                    break;
                
            }

        } finally {
            new Closer( reader ).close();
        }

    }

    /*

       TODO:

       - ability to name columns so that the result shows up correctly in the
         output with named columns.

       - printf format specifiers %02d would be nice.  Maybe just use printf
         formatters directly.

       - byte[] needs encoding mechanisms.

       - perhaps store the record format WITH the files so that we never have
         the type system when cating a file.

       - Maybe split them on format specifiers.  For example %d,%d,%2.2f 
         
     */
    private static String format( String render, StructReader value ) {

        StringBuilder buff = new StringBuilder();

        /*
         * byte
         * short
         * varint
         * int
         * long
         * float
         * double
         * boolean
         * char
         * string
         * hashcode
         */
             
        for ( String r : render.split( "," ) ) {

            if ( buff.length() > 0 )
                buff.append( " " );

            if ( r.matches( "(?i)%.*d" ) ) {
                buff.append( String.format( r, value.readInt() ) );
                continue;
            }

            if ( r.matches( "(?i)%.*f" ) ) {
                buff.append( String.format( r, value.readDouble() ) );
                continue;
            }

            if ( r.matches( "(?i)%.*s" ) ) {
                buff.append( String.format( r, value.readString() ) );
                continue;
            }

            //TODO we could use reflection here.  Java really needs syntactic
            //sugar for reflection.
            
            if ( "byte".equals( r ) ) {
                buff.append( value.readByte() );
                continue;
            }

            if ( "short".equals( r ) ) {
                buff.append( value.readShort() );
                continue;
            }

            if ( "varint".equals( r ) ) {
                buff.append( value.readVarint() );
                continue;
            }

            if ( "long".equals( r ) ) {
                buff.append( value.readLong() );
                continue;
            }

            if ( "float".equals( r ) ) {
                buff.append( value.readFloat() );
                continue;
            }

            if ( "double".equals( r ) ) {
                buff.append( String.format( "%f", value.readDouble() ) );
                continue;
            }

            if ( "boolean".equals( r ) ) {
                buff.append( value.readBoolean() );
                continue;
            }

            if ( "char".equals( r ) ) {
                buff.append( value.readChar() );
                continue;
            }

            if ( "string".equals( r ) ) {
                buff.append( value.readString() );
                continue;
            }

            if ( "int".equals( r ) ) {
                buff.append( value.readInt() );
                continue;
            }

            if ( "byte".equals( r ) ) {
                buff.append( value.readByte() );
                continue;
            }

            if ( "hashcode".equals( r ) ) {
                buff.append( Base64.encode( value.readHashcode() ) );
                continue;
            }

            if ( "wrapped:string".equals( r ) ) {

                List<StructReader> unwrapped = StructReaders.unwrap( value );

                for( StructReader u : unwrapped ) {
                    buff.append( u.readString() + " " );
                }

                continue;
            }

            if ( "wrapped:hashcode".equals( r ) ) {

                List<StructReader> unwrapped = StructReaders.unwrap( value );

                for( StructReader u : unwrapped ) {
                    buff.append( Base64.encode( u.readHashcode() ) + " " );
                }

                continue;
            }

            buff.append( String.format( "%s:", value.length() ) );
            buff.append( Base64.encode( value.toByteArray() ) );

        }

        return buff.toString();
        
    }

}

