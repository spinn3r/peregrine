
package peregrine.reduce;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.io.*;

public interface SortEntryFactory  {
    
    public SortEntry newSortEntry( byte[] key, byte[] value );

}
