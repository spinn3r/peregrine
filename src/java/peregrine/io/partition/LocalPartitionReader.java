/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.io.partition;

import java.io.*;
import java.math.*;
import java.util.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.util.*;
import peregrine.io.sstable.*;
import peregrine.os.*;
import peregrine.rpc.*;
import peregrine.task.*;
import peregrine.sort.*;

/**
 * Read data from a partition from local storage from the given file with a set
 * of chunks.
 */
public class LocalPartitionReader extends BaseJobInput implements SequenceReader, JobInput {

    private String path = null;

    private List<DefaultChunkReader> chunkReaders = new ArrayList();

    // iterator to handle 
    private Iterator<DefaultChunkReader> iterator = null;

    // the current chunk reader we are on.
    private DefaultChunkReader chunkReader = null;

    // true when we have another record to read
    private boolean hasNext = false;

    // the partition we're reading from.
    private Partition partition;
    
    private ChunkReference chunkRef;

    // the count of all records across all chunks for the given file.
    private int count = -1;

    protected TreeMap<StructReader,DataBlockReference> dataBlockLookup = null;

    public LocalPartitionReader( Config config,
                                 Partition partition,
                                 String path ) throws IOException {
        
        this( config, partition, path, new ArrayList() );
        
    }

    public LocalPartitionReader( Config config,
                                 Partition partition,
                                 String path,
                                 ChunkStreamListener listener ) throws IOException {

        this( config, partition, path, new ArrayList() );

        addListener( listener );
        
    }
    
    public LocalPartitionReader( Config config,
                                 Partition partition,
                                 String path,
                                 List<ChunkStreamListener> listeners ) throws IOException {

        this.partition = partition;
        this.chunkReaders = LocalPartition.getChunkReaders( config, partition, path );
        this.iterator = chunkReaders.iterator();
        this.path = path;

        this.chunkRef = new ChunkReference( partition, path );

        addListeners( listeners );

        // update the count by looking at all the chunks and returning the number
        // of key/value pairs they contain.
        for( ChunkReader reader : chunkReaders ) {
            count += reader.count();
        }

        dataBlockLookup = buildDataBlockIndex();
        
    }

    private TreeMap<StructReader,DataBlockReference> buildDataBlockIndex() {

        TreeMap<StructReader,DataBlockReference> result = new TreeMap( new StrictStructReaderComparator() );

        // build the index of the ChunkReaders.

        byte[] lastKey = new byte[0];
        
        for ( int idx = 0; idx < chunkReaders.size(); ++idx ) {

            DefaultChunkReader current = chunkReaders.get( idx );
            
            for( DataBlock db : current.getDataBlocks() ) {

                DataBlockReference ref = new DataBlockReference();
                ref.reader = current;
                ref.idx = idx;
                ref.dataBlock = db;

                result.put( StructReaders.wrap( db.firstKey ), ref );
            }

            if ( current.getTrailer().getCount() > 0 ) {
                lastKey = current.getFileInfo().getLastKey();
            }
            
        }

        BigInteger ptr = new BigInteger( lastKey );
        ptr = ptr.add( BigInteger.valueOf( 1 ) );
        
        byte[] pd = ptr.toByteArray();
        
        // NOTE: BigInteger is conservative with padding so we have
        // to add any byte padding back in.
        if ( pd.length < lastKey.length ) {
            
            byte[] tmp = new byte[lastKey.length];
            System.arraycopy( pd, 0, tmp, lastKey.length - pd.length, pd.length );
            pd = tmp;
            
        }
        
        result.put( new StructReader( pd ), null );

        return result;
                    
    }
    
    public List<DefaultChunkReader> getDefaultChunkReaders() {
        return chunkReaders;
    }
    
    @Override
    public boolean hasNext() throws IOException {

        if ( chunkReader != null )
            hasNext = chunkReader.hasNext();

        if ( hasNext == false ) {

            fireOnChunkEnd( chunkRef );
            
            if ( iterator.hasNext() ) {

                chunkRef.incr();
                
                chunkReader = iterator.next();

                fireOnChunk( chunkRef );
                
                hasNext = chunkReader.hasNext();

            } else {
                hasNext = false;
            }

        }

        return hasNext;
        
    }

    @Override
    public void next() throws IOException {
       	chunkReader.next();    	
    }
    
    @Override
    public StructReader key() throws IOException {
        return chunkReader.key();
    }

    @Override
    public StructReader value() throws IOException {
        return chunkReader.value();
    }

    @Override
    public void close() throws IOException {

        //TODO: migrate to using IdempotentCloser ...
        
        if ( chunkReader != null ) {
            fireOnChunkEnd( chunkRef );
        }

        new Closer( chunkReaders ).close();
        
    }

    @Override
    public String toString() {

        int offset = 0;

        if ( chunkReader != null )
            offset = chunkReader.index();

        Message message = new Message();

        message.put( "path",       path );
        message.put( "partition",  partition.getId() );
        message.put( "chunkRef",   chunkRef );
        message.put( "offset",     offset );

        return message.toString();

    }

    @Override
    public int count() {
        return count;
    }

    // contains a data block and a chunk reference and all the metadata needed
    // to find a data block and seekTo the position of a key and optionally
    // scan.
    class DataBlockReference {

        // keeps a pointer to the chunk this data block is stored 
        protected DefaultChunkReader reader = null;

        // the position in the main chunk reader list so that we can call
        // subList and get a new iterator for this and all subsequent chunks.
        protected int idx = -1;

        // the data block for this reference.  Used so that we can call seekTo
        // within the actual chunk.
        protected DataBlock dataBlock = null;
        
    }
    
}
