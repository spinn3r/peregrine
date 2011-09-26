package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;

public class SortResult {

    public int idx = 0;

    public SortEntry last = null;

    private SortMerger merger = null;

    private SortListener listener = null;
    
    public SortResult( SortMerger merger,
                       SortListener listener )
    {
        this.merger = merger;
        this.listener = listener;
    }

    public void accept( long cmp, SortEntry entry, ChunkWriter writer ) {

        //FIXME: change the handler for this (or something) so taht we're not
        //constantly checking for if last==null as we really on need to do this
        //this first pass and it's a waste of CPU.
        
        if ( last == null || last.cmp( entry ) != 0 ) {

            if ( last != null && listener != null ) {
                listener.onFinalValue( entry.key , entry.values );
            }

            last = entry;

            writer.write( entry.key, entry.values );
            
        } 

        //merger.merge( last, entry );
        
    }

}
