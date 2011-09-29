package maprunner.io;

import maprunner.*;
import maprunner.util.*;
import maprunner.keys.*;

public final class JoinedTuple {

    public byte[] key = null;
    public byte[][] values = null;
    
    public JoinedTuple( byte[] key, byte[][] values ) {

        this.key = key;
        this.values = values;
        
    }

}
