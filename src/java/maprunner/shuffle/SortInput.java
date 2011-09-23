
package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;

public final class SortInput {

    public SortRecord value;
    public int idx = 0;
    public SortRecord[] vect;
    
    public SortInput( SortRecord[] vect ) {
        this.vect = vect;
        this.value = vect[0];
    }

}
