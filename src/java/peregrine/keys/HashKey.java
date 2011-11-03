package peregrine.keys;

import peregrine.util.*;

public class HashKey extends BaseKey {

    public HashKey( String str ) {
        super( Hashcode.getHashcode( str ) );
    }

}
