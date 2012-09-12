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
package peregrine.util;

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
     * Split a string and return the result as a list.
     */
    public static List<String> split( String str , String sep ) {
        return Arrays.asList( str.split( sep ) );
    }
    
    
}
