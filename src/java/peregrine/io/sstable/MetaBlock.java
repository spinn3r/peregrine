package peregrine.io.sstable;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.os.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.io.chunk.*;

import org.jboss.netty.buffer.*;

public class MetaBlock extends BaseBlock {

    // FIXME: we need a set of key/value pairs for metadata here and also we
    // need to write them out.

    // used to keey track of key value pairs for this block
    private List<Record> records = new ArrayList();
    
     private void addRecord( StructReader key, StructReader value ) {
         records.add( new Record( key, value ) ); 
     }
    
    @Override
    public void read( ChannelBuffer buff ) {

        super.read( buff );

        StreamReader reader = new StreamReader( buff );

        for( int i = 0; i < count; ++i ) {
            records.add( DefaultChunkReader.read( reader ) );
        }
        
    }

    @Override
    public void write( ChannelBufferWritable writer ) throws IOException {
        count = records.size();
        super.write( writer );

        for( Record record : records ) {
            //FIXME just call this on the raw stream from the parent... 
            //DefaultChunkWriter.write( writer, record.getKey(), record.getValue() );
        }
        
    }

}