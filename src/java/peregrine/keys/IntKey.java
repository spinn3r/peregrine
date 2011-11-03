package peregrine.keys;

import peregrine.util.primitive.LongBytes;

public class IntKey extends BaseKey {

    public int value = 0;

    public IntKey() {}
    
    public IntKey( int value ) {
        super( LongBytes.toByteArray( value ) );
        this.value = value;
    }

    public IntKey( byte[] value ) {
        super( value );
        this.value = (int)LongBytes.toLong( value );
    }

    public String toString() {
        return Integer.toString( value );
    }
    
}