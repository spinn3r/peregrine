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
package peregrine.io.util;

import peregrine.*;
import peregrine.util.*;
import peregrine.io.*;
import peregrine.io.chunk.*;

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
public class FSCat {

    public static void main( String[] args ) throws Exception {

        String path = args[0];

        String render = "base64";

        if ( args.length == 2 ) {
            render = args[1];
        }

        SequenceReader reader = new DefaultChunkReader( new File( path ) );

        while ( reader.hasNext() ) {

            reader.next();

            StructReader key   = reader.key();
            StructReader value = reader.value();

            System.out.printf( "%s = %s\n", Base64.encode( key.toByteArray() ), format( render, value ) );

        }
        
    }

    public static String format( String render, StructReader value ) {

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
                buff.append( ", " );

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
                buff.append( value.readDouble() );
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

            buff.append( Base64.encode( value.toByteArray() ) );

        }

        return buff.toString();
        
    }

}

