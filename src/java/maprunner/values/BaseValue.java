package maprunner.values;

import java.io.*;
import java.util.*;

import maprunner.*;

public class BaseValue implements Value {

    private byte[] data;
    
    public BaseValue( byte[] data ) {
        this.data = data;
    }
    
    public byte[] toBytes() {
        return data;
    }
    
}