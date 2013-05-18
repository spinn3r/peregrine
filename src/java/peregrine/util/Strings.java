/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package peregrine.util;

import java.io.*;
import java.util.*;

/**
 * Various utility methods for working with strings.
 */
public class Strings {

    /**
     * Convert the give list of Strings into an array of Strings.
     */
    public static String[] toArray( List<String> list ) {

        String[] result = new String[ list.size() ];
        result = list.toArray( result );

        return result;

    }

    public static List<String> toList( String[] array ) {

        List<String> result = new ArrayList( array.length );

        for ( String current : array ) {
            result.add( current );
        }

        return result;

    }

    public static String join( String[] in, String sep ) {
        return join( toList( in ), sep );
    }

    /**
     * Join the given strings , adding a separator between them.
     */
    public static String join( List<String> list, String sep ) {

        StringBuilder buff = new StringBuilder();
        
        for( int i = 0; i < list.size(); ++i ) {

            if ( i > 0 )
                buff.append( sep );

            buff.append( list.get( i ) );
                
        }

        return buff.toString();
        
    }

    /**
     * Construct a path from the given strings, ignoring any null portions.  We
     * also make sure to not include / components twice if two adjacent
     * components have slashes.
     *
     */
    public static String path( Object... args ) {

        StringBuilder buff = new StringBuilder();

        for ( Object obj : args ) {

            if ( obj == null )
                continue;

            String arg = obj.toString();

            if ( buff.length() > 0 && buff.charAt( buff.length() - 1 ) != '/' ) {
                buff.append( '/' );
            }

            buff.append( arg );

        }

        return buff.toString();

    }

    /**
     * Split a string and return the result as a list.
     */
    public static List<String> split( String str , String sep ) {
        return Arrays.asList( str.split( sep ) );
    }

    public static String format( Throwable t ) {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        t.printStackTrace( new PrintStream( out ) );

        return new String( out.toByteArray() );

    }

    public static boolean empty( String str ) {
        return str == null || "".equals( str );
    }

}
