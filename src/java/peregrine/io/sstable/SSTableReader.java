package peregrine.io.sstable;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.os.*;

import org.jboss.netty.buffer.*;

public class SSTableReader {

    protected MappedFileReader reader;

    // information about the file we are writing to...
    protected FileInfo fileInfo = new FileInfo();

    // trailer information for the file
    protected Trailer trailer = new Trailer();

    protected List<DataBlock> dataBlocks = new ArrayList();

    protected List<MetaBlock> metaBlocks = new ArrayList();

    public SSTableReader( MappedFileReader reader ) throws IOException {

        this.reader = reader;

        ChannelBuffer buff = reader.map();

        // read the trailer
        trailer.read( buff );
        
        // read the file info
        fileInfo.read( buff, trailer );
        
        // read data and meta index

        buff.readerIndex( (int)trailer.indexOffset );

        for( int i = 0; i < trailer.indexCount; ++i ) {

            DataBlock db = new DataBlock();
            MetaBlock mb = new MetaBlock();

            db.read( buff );
            mb.read( buff );
            
        }
        
    }
    
}