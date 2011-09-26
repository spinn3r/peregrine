package maprunner;

import java.io.*;
import java.util.*;

import maprunner.util.*;
import maprunner.keys.*;
import maprunner.values.*;

/**
 */
public class KeyValuePair {

    public byte[] key;
    public byte[] value;
    
    public KeyValuePair( byte[] key, byte[] value ) {
        this.key = key;
        this.value = value;
    }
    
}