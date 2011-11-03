package peregrine.values;

import peregrine.util.*;
import peregrine.util.primitive.IntBytes;

public class IntValue extends BaseValue {

    public int value;

    public IntValue() {}
    
    public IntValue( int value ) {
        super( IntBytes.toByteArray( value ) );
        this.value = value;
    }

    public IntValue( byte[] value ) {
        super( value );
        this.value = IntBytes.toInt( value );
    }

    public void fromBytes( byte[] data ) {
        this.data = data;
        this.value = IntBytes.toInt( data );
    }

    public String toString() {
        return Integer.toString( value );
    }
    
}