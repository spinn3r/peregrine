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

    private boolean raw;
    
    public SortResult( int size,
                       SortMerger merger,
                       boolean raw ){
        this.entries = new SortEntry[ size ];
        this.merger = merger;
        this.raw = raw;
    }

    public void accept( long cmp, SortRecord record ) {

        if ( last == null || last.longValue() - record.longValue() != 0 ) {

            System.out.printf( "Creating new record for: %s\n", record );

            //FIXME: would be nice to just change this function
            if ( raw ) {
                last = new SortEntry( record );
            } else {
                SortEntry template = (SortEntry) record;
                last = new SortEntry();
                last.keycmp = template.keycmp;
                last.key = template.key;
            }

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
