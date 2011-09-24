package maprunner;

import java.io.*;
import java.util.*;

public interface OutputCollector {

    public void collect( byte[] key, byte[] value );
    
}
