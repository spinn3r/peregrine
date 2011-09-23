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
    
    public SortResult( int size, SortMerger merger ){
        this.entries = new SortEntry[ size ];
        this.merger = merger;
    }

    public void accept( long cmp, SortRecord record ) {

        //FIXME: aren't we doing this comparison TWICE? 
        if ( last == null || cmp != 0 ) {
            last = new SortEntry( record );
            entries[idx++] = last;
        } 

        merger.merge( last, record );
        
    }

    public SortRecord[] getRecords() {

        SortRecord[] result = new SortRecord[ idx + 1 ];
        System.arraycopy( entries, 0, result, 0, result.length );

        return result;
                         
    }
    
    public void dump() {

        for( int i = 0; i < idx; ++i ) {

            //System.out.printf( "key=%s, size=%,d ", new IntKey( entries[i].record.key ).value, entries[i].values.size() );
            //System.out.printf( "key=%s, size=%,d ", entries[i].record, entries[i].values.size() );
            System.out.printf( "{" );

            for( byte[] value : entries[i].values ) {
                System.out.printf( "%,d, ", new IntValue( value ).value );
            }

            System.out.printf( "}\n" );

        }
        
    }
    
}
