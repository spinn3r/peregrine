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
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.*;
import java.net.*;
import java.security.*;

/**
 * <p>
 * Simple getopt implementation which doesn't have many features but works for
 * 80% of common usage and doesn't require an external library.
 *
 * <p>
 * 
 * 
 */
public class Getopt {

    // TODO refactor this to use StructMap as the backend.
    private Map<String,String> params = new HashMap();
    
    private List<String> values = new ArrayList();
    
    /**
     * Parse command line arguments like --foo=bar where foo is the key and bar
     * is the value.
     *
     */
    public Getopt( String[] args ) {

        for( String arg : args ) {

            if ( ! arg.startsWith( "--" ) ) {
                values.add( arg );
                continue;
            }
            
            String[] split = arg.split( "=" );

            String key = split[0];

            //strip the -- prefix
            if ( key.startsWith( "--" ) ) {
                key = key.substring( 2, key.length() );
            }

            if ( split.length != 2 ) {
                // make this a boolean
                params.put( key, "true" ) ;
                continue;
            }

            String value = split[1];

            params.put( key, value );
            
        }

    }

    public boolean getBoolean( String name ) {
        return getBoolean( name, false );
    }

    public boolean getBoolean( String name, boolean _default ) {

        if ( params.containsKey( name ) )
            return "true".equals( params.get( name ) );

        return _default;

    }

    public long getLong( String name ) {
        return getLong( name, 0 );
    }

    public long getLong( String name, long _default ) {

        if ( params.containsKey( name ) )
            return Long.parseLong( params.get( name ) );

        return _default;
        
    }

    public int getInt( String name ) {
        return getInt( name, 0 );
    }

    public int getInt( String name, int _default ) {

        if ( params.containsKey( name ) )
            return Integer.parseInt( params.get( name ) );

        return _default;

    }

    public String getString( String name ) {
        return getString( name, null );
    }

    public String getString( String name, String _default ) {

        if ( params.containsKey( name ) )
            return params.get( name );

        return _default;

    }

    public boolean containsKey( String key ) {
        return params.containsKey( key );
    }

    /**
     * Require all the given keys to be specified.
     */
    public void require( String... keys ) {

        for( String key : keys ) {

            if ( ! containsKey( key ) ) {
                throw new RuntimeException( String.format( "Key %s not specified", key ) );
            }
            
        }
        
    }
    
    public Map<String,String> getParams() {
        return params;
    }

    public List<String> getValues() {
        return values;
    }

    public static void main( String[] args ) {

        //example usage.
        
        Getopt getopt = new Getopt( args );

        Map<String,String> params = getopt.getParams();
        List<String> values = getopt.getValues();

    }
    
}
