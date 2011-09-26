
package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;

/**
 * Handles taking an input value and merging it with an existing value.  We need
 * two functions.  One for original (K,V) pairs and one for intermediate
 * (K,V...) pairs.
 */
public class SortMerger {

//     public void merge( SortEntry entry, SortRecord record ) {
//         entry.values.add( ((Tuple)record).value );
//     }

//     public SortEntry newSortEntry( SortRecord record ) {
//         //return new SortEntry( record );
//     }

}