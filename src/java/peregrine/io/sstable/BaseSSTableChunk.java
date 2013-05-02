package peregrine.io.sstable;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.os.*;

/**
 * Methods common to all SSTable chunk readers and writers.
 */
public abstract class BaseSSTableChunk {

    // information about the file we are writing to...
    protected FileInfo fileInfo = new FileInfo();

    // trailer information for the file
    protected Trailer trailer = new Trailer();

    // list of data blocks in the index.
    protected List<DataBlock> dataBlocks = new ArrayList();

    // list of meta blocks in the index
    protected List<MetaBlock> metaBlocks = new ArrayList();

    public Trailer getTrailer() {
        return trailer;
    }

    public FileInfo getFileInfo() {
        return fileInfo;
    }

    public List<DataBlock> getDataBlocks() {
        return dataBlocks;
    }

    public List<MetaBlock> getMetaBlocks() {
        return metaBlocks;
    }

}