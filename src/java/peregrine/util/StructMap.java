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

/**
 * Simple map for dealing with primitive types stored in a type or anonymous
 * map.
 */
public class StructMap {

    private static final Map<String,Long> SIZES = new HashMap() {{

        put( "K", (long) Math.pow(2, 10) );
        put( "M", (long) Math.pow(2, 20) );
        put( "G", (long) Math.pow(2, 30) );
        put( "T", (long) Math.pow(2, 40) );
        
    }};
    
    protected Map delegate = new HashMap();

    protected List<String> keys = new ArrayList();

    public StructMap() {}

    public StructMap( InputStream is ) throws IOException {

        Properties props = new Properties();
        props.load( is );
        delegate = props;

        init();

    }
    
    public StructMap( Map delegate ) {
        this.delegate = delegate;
        init();
    }

    private void init() {

        for( Object key : delegate.keySet() ) {
            keys.add( key.toString() );
        }

    }
    
    public void put( String key, String value ) {
        keys.add( key );
        delegate.put( key, value );
    }

    public void put( String key, int value ) {
        keys.add( key );
        delegate.put( key, ""+value );
    }


    public void put( String key, Object value ) {
    	put( key, value.toString() );
    }
    
    public void put( String key, Throwable throwable ) {
    	
        // include the full stack trace 
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        throwable.printStackTrace( new PrintStream( out ) );

        String stacktrace = new String( out.toByteArray() );
    	
        put( key, stacktrace );
        
    }
    
    /**
     * Put a list of values which can later be read as an ordered list of values.
     */
    public void put( String prefix, List list ) {

        if ( list == null )
        	return;
            
        int idx = 0;
        for( Object val : list ) {
        	put( prefix + "." + idx++, val );
        }
    	
    }
    
    public boolean getBoolean( String key ) {

        if ( delegate.containsKey( key ) )
            return "true".equals( delegate.get( key ) );

        return false;

    }

    public int getInt( String key ) {

        return getInt( key , 0 );
        
    }

    public int getInt( String key, int _default ) {

        if ( delegate.containsKey( key ) )
            return Integer.parseInt( delegate.get( key ).toString() );

        return _default;

    }

    public void setInt( String key, int value ) {
        delegate.put( key, Integer.toString( value ) );
    }

    public long getSize( String key ) {

        String value = get( key );

        if ( value.matches( "[0-9]+[a-zA-Z]" ) ) {
        
            int len = value.length();
            
            String suffix;
            suffix = value.substring( len - 1, len );
            suffix = suffix.toUpperCase();
        
            String prefix = value.substring( 0, len - 1 );
            
            long result = Long.parseLong( prefix );
            
            return result * SIZES.get( suffix );

        } else {
            return getLong( key );
        }

    }
    
    public long getLong( String key ) {

        if ( delegate.containsKey( key ) )
            return Long.parseLong( delegate.get( key ).toString() );

        return 0L;

    }
    
    public List getList( String prefix ) {
    	
        List result = new ArrayList();
        
        for( int i = 0 ; i < Integer.MAX_VALUE; ++i ) {

            Object val = get( prefix + "." + i );

            if ( val == null )
                break;

            result.add( val );
            
        }

        return result;
    }

    public String get( String key ) {
        return get( key, null );        
    }

    public String get( String key, String _default ) {

        if ( delegate.containsKey( key ) )
            return delegate.get( key ).toString();

        return _default;

    }

    public void set( String key, String value ) {
        delegate.put( key, value );
    }

    public String getString( String key ) {
        return get( key );
    }

    public List<String> getKeys() {

        List<String> result = new ArrayList();
        result.addAll( keys );
        return result;
        
    }

    public boolean containsKey( String key ) {
        return delegate.containsKey( key );
    }

    /**
     * Return the number of entries.
     */
    public int size() {
        return delegate.size();
    }
    
    @Override
    public String toString() {
        return delegate.toString();
    }
    
}
