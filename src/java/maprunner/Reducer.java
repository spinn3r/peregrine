package maprunner;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.shuffle.*;

public class Reducer {

    public List<byte[]> reduce( long global_chunk_id,
                                byte[] key,
                                List<byte[]> values ) {

        //identity by default...
        return values;
        
    }

}
