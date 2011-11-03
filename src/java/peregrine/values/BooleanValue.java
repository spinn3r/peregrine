package peregrine.values;

import peregrine.*;

public class BooleanValue implements Value {

    public static final byte[] TRUE  = new byte[] { (byte)1 };
    public static final byte[] FALSE = new byte[] { (byte)0 };
    
    public boolean value;

    public BooleanValue() {}
    
    public BooleanValue( boolean value ) {
        this.value = value;
    }

    public BooleanValue( byte[] value ) {
        fromBytes( value );
    }

    public void fromBytes( byte[] data ) {
        this.value = data[0] == 1;
    }

    public byte[] toBytes() {
        return this.value ? TRUE : FALSE;
    }

    public String toString() {
        return value ? "true" : "false";
    }
    
}