package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;
import maprunner.io.*;

public class SortResult {

    public int idx = 0;

    public SortEntry last = null;

    private SortListener listener = null;

    private ChunkWriter writer = null;
    
    public SortResult( ChunkWriter writer,
                       SortListener listener )
    {
        this.listener = listener;
        this.writer = writer;
    }

    public void accept( SortEntry entry ) throws IOException {

        System.out.printf( "FIXME: emit accept thread=%s key=%s\n", Thread.currentThread().getId(), Hex.encode( entry.key ) );

        FullKeyComparator comparator = new FullKeyComparator();
        
        if ( last == null || comparator.compare( last.key, entry.key ) != 0 ) {

            if ( last != null )
                System.out.printf( "FIXME: emit thread=%s last=%s vs entry=%s\n",
                                   Thread.currentThread().getId(),
                                   Hex.encode( last.key ),
                                   Hex.encode( entry.key ) );

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

        if ( entry == null )
            return;
        
        if ( listener != null ) {

            System.out.printf( "FIXME: onFinalValue %s key=%s\n", Thread.currentThread().getId(), Hex.encode( entry.key ) );

            listener.onFinalValue( entry.key , entry.getValues() );
        }

        ByteArrayListValue intervalue = new ByteArrayListValue();

        intervalue.addValues( entry.getValues() );

        if ( writer != null )
            writer.write( entry.key, intervalue.toBytes() );

    }
    
}
