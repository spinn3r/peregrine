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
import java.util.concurrent.*;

import peregrine.rpc.*;

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
    
    protected Map delegate = new ConcurrentHashMap();

    protected Queue<String> keys = new LinkedBlockingQueue();

    public StructMap() {}

    public StructMap( InputStream is ) throws IOException {

        Properties props = new Properties();
        props.load( is );
        init( props );

    }
    
    public StructMap( Map delegate ) {
        init( delegate );
    }

    public void init( Map delegate ) {

        this.delegate = delegate;
        
        for( Object key : delegate.keySet() ) {
            keys.add( key.toString() );
        }

    }
    
    public void put( String key, String value ) {

        if ( value == null )
            return;
        
        keys.add( key );
        delegate.put( key, value );
    }

    public void put( String key, int value ) {
        keys.add( key );
        delegate.put( key, ""+value );
    }

    public void put( String key, long value ) {
        keys.add( key );
        delegate.put( key, ""+value );
    }

    public void put( String key, Class value ) {

        if ( value == null )
            return;

        put( key, value.getName() );
        
    }

    public void put( String key, Object value ) {

        if ( value == null )
            return;

        if ( value instanceof MessageSerializable ) {
            
            MessageSerializable ms = (MessageSerializable)value;
            put( key, ms.toMessage().toString() );
            return;
        }

    	put( key, value.toString() );

    }
    
    public void put( String key, Throwable throwable ) {

        put( key, Strings.format( throwable ) );
        
    }
    
    /**
     * Put a list of values which can later be read as an ordered list of values.
     */
    public void put( String prefix, Collection list ) {

        if ( list == null )
        	return;
            
        int idx = 0;
        for( Object val : list ) {

            String key = prefix + "." + idx++;
            
            if ( val instanceof MessageSerializable ) {

                MessageSerializable ms = (MessageSerializable)val;
                put( key, ms.toMessage() );

            } else {
                put( key, val );
            }

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

    public long getSize( String key ) {
        return getSize( key, 0L );
    }

    public long getSize( String key, long _default ) {

        String value = get( key );

        if ( value == null )
            return _default;
        
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
    
    public List<String> getList( String prefix ) {
    	
        List<String> result = new ArrayList();
        
        for( int i = 0 ; i < Integer.MAX_VALUE; ++i ) {

            String val = get( prefix + "." + i );

            if ( val == null )
                break;

            result.add( val );
            
        }

        return result;
    }

    public List getList( String prefix, Class clazz ) {

        return new StructList( getList( prefix ), clazz );

    }

    public String get( String key ) {
        return get( key, null );        
    }

    public String get( String key, String _default ) {

        if ( delegate.containsKey( key ) )
            return delegate.get( key ).toString();

        return _default;

    }

    public String getString( String key ) {
        return get( key );
    }

    public Class getClass( String key ) {

        if ( ! containsKey( key ) )
            return null;
        
        try {
            return Class.forName( getString( key ) );
        } catch ( ClassNotFoundException e ) {
            throw new RuntimeException( e );
        }
        
    }
    
    public Set<String> getKeys() {

        Set<String> result = new TreeSet();
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

    public Map toMap() {
        //return a copy of the internal delegate.
        return new HashMap( delegate );
    }
    
    /**
     * Return this struct as a dictionary.
     */
    public Map<String,String> toDict() {

        Map<String,String> result = new HashMap();

        for( Object key : delegate.keySet() ) {
            result.put( key.toString(), delegate.get( key ).toString() );
        }

        return result;
        
    }
    
    @Override
    public String toString() {
        return delegate.toString();
    }
    
}
