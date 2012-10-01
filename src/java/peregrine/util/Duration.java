/*
 * Copyright 2012 Kevin A. Burton
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

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Render a given date value as a human readable string.
 */
public class Duration {

    private long delta;

    private long value;

    private Map<String,Integer> map = new HashMap();
    
    public Duration( long delta ) {
        this.delta = delta;
        this.value = delta;
        parse();
    }

    private void update( long interval, String name ) {

        int result = (int)Math.floor( value / interval );
        value -= (result * interval);
        map.put( name, result );
        
    }
    
    /**
     */
    public Map<String,Integer> parse() {

        map.put( "days",         0 );
        map.put( "hours",        0 );
        map.put( "minutes",      0 );
        map.put( "seconds",      0 );
        map.put( "milliseconds", 0 );
        
        update( 24 * 60 * 60 * 1000, "days"         );
        update(      60 * 60 * 1000, "hours"        );
        update(           60 * 1000, "minutes"      );
        update(                1000, "seconds"      );
        update(                   1, "milliseconds" );

        return map;
    }

    /**
     * Format a duration in hh:mm:ss format.
     */
    @Override
    public String toString() {

        StringBuilder buff = new StringBuilder();

        int days = map.get( "days" );
        
        if ( days > 0 ) {
            buff.append( String.format( "%sd ", days ) );
        }
        
        buff.append( String.format( "%02d:%02d:%02d",
                                    map.get( "hours" ),
                                    map.get( "minutes" ),
                                    map.get( "seconds" ) ) );

        return buff.toString();

    }
    
    public static void main( String[] args ) throws Exception {
        System.out.println( new Duration( 23 * 60 * 60 * 1000 ).toString() );
    }

}