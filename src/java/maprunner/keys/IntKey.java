package maprunner.keys;

import java.io.*;
import java.util.*;

import maprunner.*;
import maprunner.util.*;

public class IntKey extends BaseKey {

    public int value = 0;
    
    public IntKey( int value ) {
        super( IntBytes.toByteArray( value ) );
        this.value = value;
    }

    public IntKey( byte[] value ) {
        super( value );
        this.value = IntBytes.toInt( value );
    }

    public String toString() {
        return Integer.toString( value );
    }
    
}