package maprunner;

import maprunner.util.*;
import maprunner.keys.*;
import maprunner.shuffle.SortRecord;

public class Tuple {

    public byte[] key = null;
    public byte[] value = null;

    public Tuple( byte[] key, byte[] value ) {

        this.key = key;
        this.value = value;
        
    }

    public Tuple( Key key, Value value ) {

        this.key = key.toBytes();
        this.value = value.toBytes();

    }

}
