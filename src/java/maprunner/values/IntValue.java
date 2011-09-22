package maprunner.values;

import java.io.*;
import java.util.*;

import maprunner.*;
import maprunner.util.*;

public class IntValue extends BaseValue {

    public int value;
    
    public IntValue( int value ) {
        super( IntBytes.toByteArray( value ) );
        this.value = value;
    }

    public IntValue( byte[] value ) {
        super( value );
        this.value = IntBytes.toInt( value );
    }

}