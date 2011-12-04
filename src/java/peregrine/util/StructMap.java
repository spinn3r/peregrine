
package peregrine.util;

import java.util.*;

/**
 * Simple map for dealing with primitive types stored in a type or anonymous
 * map.
 */
public class StructMap {

    private static final Map<Character,Long> SIZES = new HashMap() {{

        put( 'K', (long) Math.pow(2, 10) );
        put( 'M', (long) Math.pow(2, 20) );
        put( 'G', (long) Math.pow(2, 30) );
        put( 'T', (long) Math.pow(2, 40) );
        
    }};
    
    protected Map delegate = new HashMap();

    protected List<String> keys = new ArrayList();
    
    public StructMap() {}
    
    public StructMap( Map delegate ) {
        this.delegate = delegate;
    }

    public void put( String key, String value ) {
        keys.add( key );
        delegate.put( key, value );
    }

    public void put( String key, int value ) {
        keys.add( key );
        delegate.put( key, ""+value );
    }

    public boolean getBoolean( String key ) {

        if ( delegate.containsKey( key ) )
            return "true".equals( delegate.get( key ) );

        return false;

    }

    public int getInt( String key ) {

        if ( delegate.containsKey( key ) )
            return Integer.parseInt( delegate.get( key ).toString() );

        return 0;

    }

    public long getSize( String key ) {

        String value = get( key );
        
        char suffix = value.charAt( value.length() - 1 );

        String prefix = value.substring( 0, value.length() - 1 );

        long result = Long.parseLong( prefix );

        return result * SIZES.get( suffix );
        
    }
    
    public long getLong( String key ) {

        if ( delegate.containsKey( key ) )
            return Long.parseLong( delegate.get( key ).toString() );

        return 0L;

    }

    public String get( String key ) {

        if ( delegate.containsKey( key ) )
            return delegate.get( key ).toString();

        return null;
        
    }

    public String getString( String key ) {
        return get( key );
    }

}