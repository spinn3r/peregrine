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

    public static String format( long val ) {
        return format( val, 4 );
    }

    public static String format( long val, int length ) {
        return format( val, length, new ArrayList() {{
            add( new Unit( 1000.0,          "K" ) );
            add( new Unit( 1000000.0,       "M" ) );
            add( new Unit( 1000000000.0,    "B" ) );
            add( new Unit( 1000000000000.0, "T" ) );
        }} );
    }

    public static String formatBytes( long val ) {
        return formatBytes( val, 4 );
    }

    /**
     * Just like format but we use 'byte' specifiers(KB,MB,GB,etc)
     */
    public static String formatBytes( long val, int length ) {
        return format( val, length, new ArrayList() {{
            add( new Unit( 1000.0,          "KB" ) );
            add( new Unit( 1000000.0,       "MB" ) );
            add( new Unit( 1000000000.0,    "GB" ) );
            add( new Unit( 1000000000000.0, "TB" ) );
        }} );
    }

    /**
     * Format a long into K,M,B units.  The val is the value we wish to format
     * and the length is the ideal length of the value without the suffix.
     */
    public static String format( long val, int length, List<Unit> units ) {

        if ( val <= 1000 )
            return "" + val;

        for ( Unit unit : units ) {

            if ( val < (unit.value * 1000.0) ) {

                double div = val / unit.value;
                String str = null;

                for( int j = 3; j >= 0; --j ) {

                    String fmt = String.format( "%%.%sf", j );
                    str = String.format( fmt, div );

                    if ( str.length() == length )
                        break;
                    
                }

                return String.format( "%s%s", str, unit.name );
                
            }
            
        }

        return "" + val;

    }

    static class Unit {

        protected double value;
        protected String name;
        
        public Unit( double value , String name ) {
            this.value = value;
            this.name = name;
        }
        
    }
    
}
