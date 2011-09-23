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

    public SortEntry[] entries = null;

    public SortEntry last = null;

    private SortMerger merger = null;

    private Reducer reducer = null;
    
    public SortResult( int size,
                       SortMerger merger,
                       Reducer reducer )
    {
        this.entries = new SortEntry[ size ];
        this.merger = merger;
        this.reducer = reducer;
    }

    public void accept( long cmp, SortRecord record ) {

        //FIXME: change the handler for this (or something) so taht we're not
        //constantly checking for if last==null as we really on need to do this
        //this first pass and it's a waste of CPU.
        
        if ( last == null || last.longValue() - record.longValue() != 0 ) {

            if ( last != null && reducer != null ) {

                SortEntry entry = (SortEntry) record;
                reducer.reduce( entry.key , entry.values );

            }

            last = merger.newSortEntry( record );
            entries[idx++] = last;
        } 

        merger.merge( last, record );
        
    }

    public SortRecord[] getRecords() {

        SortRecord[] result = new SortRecord[ idx ];
        System.arraycopy( entries, 0, result, 0, result.length );

        return result;
                         
    }
    
    public void dump( SortRecord[] records ) {

        for( SortRecord record : records  ) {
            System.out.printf( "    %s\n" , record );
        }

    }
    
}
