package maprunner;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.shuffle.*;

public class Reducer {

    // tell the reducer which partition its running on as well as the host.

    public void init( Partition partition ) {

    }
    
    public List<byte[]> reduce( byte[] key, List<byte[]> values ) {

        //identity by default...
        return values;
        
    }

}
