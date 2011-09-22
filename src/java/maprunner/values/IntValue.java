package maprunner.values;

import java.io.*;
import java.util.*;

import maprunner.*;
import maprunner.util.*;

public class IntValue extends BaseValue {
    
    public IntValue( int value ) {
        super( IntBytes.toByteArray( value ) );
    }

}