package peregrine.keys;

import peregrine.*;
import peregrine.util.Hex;

public abstract class BaseKey implements Key {

    protected byte[] data;

    public BaseKey() {}
    
    public BaseKey( byte[] data ) {
        this.fromBytes( data );
    }
    
    public byte[] toBytes() {
        return data;
    }

    public void fromBytes( byte[] data ) {
        this.data = data;
    }

    public String toString() {
        return Hex.encode( data );
    }

}