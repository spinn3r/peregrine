package maprunner.util;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import maprunner.*;

/**
 *
 * A class that allow thread safe append to an Array ... and efficient toArray
 * functionality.  Basically it creates fixed chunks which get filled up and
 * then we keep filling up chunks.  At the end we copy these chunks into a
 * bigger larger chunk by System.arraycopy which is fast.
 *
 */
public class BulkArray {

    private static int SIZE = 16384;

    private Tuple[] chunk = new Tuple[ SIZE ];

    private AtomicInteger pointer = new AtomicInteger(); 
    private AtomicInteger size = new AtomicInteger(); 

    private List<Tuple[]> chunks = Collections.synchronizedList( new LinkedList() );
    
    public BulkArray() {
        chunks.add( chunk );
    }
    
    public void add( Tuple tuple ) {

        int ptr = pointer.getAndIncrement();

        // FIXME: rollover: this synchronized mechanism is very lame and I'd
        // very much like to avoid it.  There's no reason I can't make this 100%
        // lock free.

        if ( ptr >= SIZE ) {

            synchronized( chunks ) {
                if ( ptr >= SIZE ) {
                
                    chunk = new Tuple[ SIZE ];
                    chunks.add( chunk );
                    pointer.set( 0 );

                }

                ptr = pointer.getAndIncrement();

            }

        }

        chunk[ptr] = tuple;
        size.getAndIncrement();
        
    }

    public int size() {
        return size.get();
    }
    
    public Tuple[] toArray() {

        int size = this.size.get();
        
        //System.out.printf( "size: %,d\n", size );
        
        Tuple[] result = new Tuple[ size ];

        // (Object src, int srcPos, Object dest, int destPos, int length) 

        int offset = 0;
        int remaining = size;
        for( Tuple[] chunk : chunks ) {

            int length = chunk.length;
            
            if ( remaining < chunk.length ) {
                length = remaining;
            }

            //System.out.printf( "offset: %,d length: %,d\n", offset, length );
            
            System.arraycopy( chunk, 0, result, offset, length );

            offset += chunk.length;

            remaining -= chunk.length;

            
        }

        return result;
        
    }
    
}