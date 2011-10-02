package peregrine.values;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.Hex;

public abstract class BaseValue implements Value {

    protected byte[] data;

    public BaseValue() {}
    
    public BaseValue( byte[] data ) {
        fromBytes( data );
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