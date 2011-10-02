package peregrine.keys;

import java.io.*;
import java.util.*;

import java.nio.charset.Charset;

import peregrine.*;
import peregrine.util.*;

public class HashKey extends BaseKey {

    public HashKey( String str ) {
        super( Hashcode.getHashcode( str ) );
    }

}
