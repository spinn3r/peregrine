package maprunner.keys;

import java.io.*;
import java.util.*;

import maprunner.*;
import maprunner.util.*;

public class IntKey extends BaseKey {
    
    public IntKey( int value ) {
        super( IntBytes.toByteArray( value ) );
    }

}