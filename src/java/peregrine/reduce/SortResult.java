package peregrine.reduce;

import java.io.*;
import peregrine.util.*;
import peregrine.values.*;
import peregrine.io.chunk.*;

public class SortResult {

    public int idx = 0;

    public SortEntry last = null;

    private SortListener listener = null;

    private LocalChunkWriter writer = null;
    
    public SortResult( LocalChunkWriter writer,
                       SortListener listener )
    {
        this.listener = listener;
        this.writer = writer;
    }

    public void accept( SortEntry entry ) throws IOException {

        FullKeyComparator comparator = new FullKeyComparator();

        System.out.printf( "FIXME: on entry: %s\n", Hex.encode( entry.key ) );
        
        if ( last == null || comparator.compare( last.key, entry.key ) != 0 ) {

            System.out.printf( "FIXME: gonna try to emit: \n", Hex.encode( entry.key ) );
            
            emit( last );
            last = entry;

        } else {
            // merge the values together ... 
            last.addValues( entry.getValues() );
        }
        
    }

    public void close() throws IOException {

        emit( last );
            
    }

    private void emit( SortEntry entry ) throws IOException {

        if ( entry == null ) {
            return;
        }
        
        if ( listener != null ) {
            System.out.printf( "FIXME: sweet gonna call onFinalValue... \n" );
            listener.onFinalValue( entry.key , entry.getValues() );
        }

        Struct struct = new Struct();

        struct.write( entry.getValues() );

        if ( writer != null )
            writer.write( entry.key, struct.toBytes() );

        System.out.printf( "FIXME: emitted!\n" );
        
    }
    
}
