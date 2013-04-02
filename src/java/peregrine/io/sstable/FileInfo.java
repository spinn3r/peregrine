package peregrine.io.sstable;

import java.io.*;

import peregrine.*;
import peregrine.os.*;

public class FileInfo {

    protected int meanKeyLength = -1;

    protected int meanValueLength = -1;

    protected byte[] lastKey = new byte[0];

    // default is no comparator ... 
    protected byte[] comparatorClass = new byte[0];

    public int getMeanKeyLength() { 
        return this.meanKeyLength;
    }

    public void setMeanKeyLength( int meanKeyLength ) { 
        this.meanKeyLength = meanKeyLength;
    }

    public int getMeanValueLength() { 
        return this.meanValueLength;
    }

    public void setMeanValueLength( int meanValueLength ) { 
        this.meanValueLength = meanValueLength;
    }

    public void setLastKey( byte[] lastKey ) { 
        this.lastKey = lastKey;
    }

    public byte[] getLastKey() { 
        return this.lastKey;
    }

    public byte[] getComparatorClass() { 
        return this.comparatorClass;
    }

    public void setComparatorClass( byte[] comparatorClass ) { 
        this.comparatorClass = comparatorClass;
    }

    public void write( MappedFileWriter writer ) throws IOException {

        StructWriter sw = new StructWriter( 100 );

        sw.writeInt( meanKeyLength );
        sw.writeInt( meanValueLength );
        sw.writeBytes( lastKey );
        sw.writeBytes( comparatorClass );

        writer.write( sw.getChannelBuffer() );
        
    }

}