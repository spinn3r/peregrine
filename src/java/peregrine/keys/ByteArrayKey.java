package peregrine.keys;

import java.io.*;
import java.util.*;

import java.nio.charset.Charset;

import peregrine.*;
import peregrine.util.*;

public class ByteArrayKey extends BaseKey {

    public ByteArrayKey() {
    } 
    
    public ByteArrayKey( byte[] data ) {
        super( data );
    }

}