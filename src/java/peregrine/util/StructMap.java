
package peregrine.util;

import java.util.*;

/**
 * Simple map for dealing with primitive types stored in a type or anonymous
 * map.
 */
public class StructMap {

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

    public int getInt( String key ) {

        if ( delegate.containsKey( key ) )
            return Integer.parseInt( delegate.get( key ).toString() );

        return 0;

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