package maprunner.keys;

import java.io.*;
import java.util.*;

import maprunner.*;

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

}