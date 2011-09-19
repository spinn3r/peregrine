package maprunner;

import java.io.*;
import java.util.*;

import maprunner.util.*;
import maprunner.keys.*;
import maprunner.values.*;

public interface ChunkListener {

    public void onEntry( byte[] key, byte[] value );
    
}
