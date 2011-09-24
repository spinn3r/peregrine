
package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;

public class Sorter {

    private boolean finalPass = false;

    private SortListener listener = null;

    public Sorter() {
        this.listener = new SortListener(); // we don't care to listen.
    }

    public Sorter( SortListener listener ) {
        this.listener = listener;
    }
    
    public SortRecord[] sort( List<SortRecord[]> input ) {

        // we're done.
        if ( input.size() == 1 )
            return input.get( 0 );

        List<SortRecord[]> result = new ArrayList();

        if ( input.size() == 2 ) {
            finalPass = true;
        }

        int offset = 0;

        //odd sized input.  First merge the last two.
        if ( input.size() > 2 && input.size() % 2 != 0) {

            //FIXME: include the odd man out in this set.

            System.out.printf( "FIXMEL \n" );
            
            SortRecord[] left  = input.remove( input.size() - 2 );
            SortRecord[] right = input.remove( input.size() - 1 );

            input.add( sort( left, right ) );
            
        }
        
        for( int i = 0; i < input.size() / 2; ++i ) {

            offset = i * 2;
            
            SortRecord[] left  = input.get( offset );
            SortRecord[] right = input.get( ++offset );

            result.add( sort( left, right ) );
            
        }

        return sort( result );
            
    }
    
    public SortRecord[] sort( SortRecord[] vect_left,
                              SortRecord[] vect_right ) {

        SortInput left = new SortInput( vect_left );
        SortInput right = new SortInput( vect_right );

        SortMerger merger = null;

        if ( left.value instanceof Tuple ) {
            merger = new SortMerger();
        } else {
            merger = new IntermediateMerger();
        }

        SortListener sortListener = null;

        if ( finalPass ) 
            sortListener = listener;
            
        SortResult result = new SortResult( vect_left.length + vect_right.length, merger, sortListener );

        while( true ) {

            SortInput hit = null;
            SortInput miss = null;

            long cmp = left.value.longValue() - right.value.longValue();

            if ( cmp <= 0 ) {
                hit = left;
                miss = right;
            } else {
                hit = right;
                miss = left;
            }

            result.accept( cmp, hit.value );
            
            ++hit.idx;

            if ( hit.idx == hit.vect.length ) {
                //drain the data from the 'miss' so that there are no more
                //entries in it.

                for( int i = miss.idx; i < miss.vect.length; ++i ) {
                    result.accept( -1 , miss.value );
                }
                
                break;
            }

            hit.value = hit.vect[ hit.idx ];
            
        }

        SortRecord[] records = result.getRecords();
        
        return records;

    }

}

class IntermediateMerger extends SortMerger {
    
    public void merge( SortEntry entry, SortRecord record ) {
        entry.values.addAll( ((SortEntry)record).values );
    }

    public SortEntry newSortEntry( SortRecord record ) {

        SortEntry template = (SortEntry) record;
        SortEntry result = new SortEntry();
        result.keycmp = template.keycmp;
        result.key = template.key;

        return result;
        
    }

}