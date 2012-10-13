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
 * Various utility methods for working with longs.
 */
public class Longs {

    /**
     * Convert the give list of Longs into an array of Longs.
     */
    public static List<Long> toList( long[] values ) {

        List<Long> list = new ArrayList( values.length );

        for( long value : values ) {
            list.add( value );
        }

        return list;

    }

    /**
     * Format a long into K,M,B units.
     */
    public static String format( long val ) {

        if ( val <= 1000 )
            return "" + val;

        double[] units  = new double[] { 1000.0, 1000000.0, 1000000000.0 };
        String[] suffix = new String[] { "K", "M", "B" };

        for ( int i = 0; i < units.length; ++i ) {

            double u = units[i];

            if ( val < (u * 1000.0) ) {

                double div = val / u;
                String str = "" + div;

                str = str.substring( 0, str.indexOf( "." )  + 2 );

                // remove trailing zero if necessary
                if ( str.endsWith( ".0" ) ) {
                    str = str.substring( 0, str.length() - 2 );
                }
                
                String s = suffix[i];
                
                return String.format( "%s%s", str, s );
                
            }
            
        }

        return "" + val;

    }

}
