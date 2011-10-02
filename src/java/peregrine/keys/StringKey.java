package peregrine.keys;

import java.io.*;
import java.util.*;

import java.nio.charset.Charset;

import peregrine.*;

public class StringKey extends BaseKey {

    private static Charset UTF8 = null;

    public StringKey() {}
    
    public StringKey( String data ) {
        super( data.getBytes( UTF8 ) );
    }

    static {
        UTF8 = Charset.forName( "UTF-8" );
    }
    
}