
package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;
import maprunner.io.*;

public class MapOutputSorter {

    private List<ChunkReader> input = new ArrayList();

    private SortListener listener = null;

    public MapOutputSorter( SortListener listener ) {
        this.listener = listener;
    }

    public void add( ChunkReader reader ) {
        this.input.add( reader );
    }
    
    public void sort() throws Exception {

        //FIXME: this implements the DEFAULT sort everything approach not the
        //hinted pre-sorted approach which in some applications would be MUCH
        //faster for the reduce operation.

        ChunkSorter sorter = new ChunkSorter();

        List<ChunkReader> sorted = new ArrayList();

        for ( ChunkReader reader : input ) {
            sorted.add( sorter.sort( reader ) );
        }

        final AtomicInteger nr_tuples = new AtomicInteger();

        ChunkMerger merger = new ChunkMerger( listener );

        merger.merge( sorted );

    }

}
