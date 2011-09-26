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

    private SortListener listener = null;

    private ChunkWriter writer = null;
    
    public SortResult( ChunkWriter writer,
                       SortListener listener )
    {
        this.listener = listener;
        this.writer = writer;
    }

    public void accept( long cmp, SortEntry entry ) throws IOException {

        if ( last == null || last.cmp( entry ) != 0 ) {

            if ( last != null ) 
                emit( last );

            last = entry;

        } else {
            // merge the values together... 
            last.values.addAll( entry.values );
        }
        
    }

    public void close() throws IOException {
        emit( last );
    }

    private void emit( SortEntry entry ) throws IOException {

        if ( listener != null ) {
            listener.onFinalValue( entry.key , entry.values );
        }

        //FIXME: 
        //writer.write( entry.key, entry.values );
        writer.write( entry.key, new byte[0] );

    }
    
}
