package maprunner.keys;

import java.io.*;
import java.util.*;

import java.nio.charset.Charset;

import maprunner.*;
import maprunner.util.*;

public class ByteArrayKey extends BaseKey {

    public ByteArrayKey() {
    } 
    
    public ByteArrayKey( byte[] data ) {
        super( data );
    }

    public String toString() {
        return Base64.encode( data );
    }

}