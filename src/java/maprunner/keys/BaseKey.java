package maprunner.keys;

import java.io.*;
import java.util.*;

import maprunner.*;

public class BaseKey implements Key {

    protected byte[] data;
    
    public BaseKey( byte[] data ) {
        this.data = data;
    }
    
    public byte[] toBytes() {
        return data;
    }
    
}