/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import peregrine.metrics.RegionMetrics;
import peregrine.rpc.*;
import peregrine.sort.*;
import peregrine.util.Hex;
import peregrine.worker.clientd.requests.BackendRequest;
import peregrine.worker.clientd.requests.ScanBackendRequest;

/**
 * Read data from a partition from local storage from the given file with a set
 * of chunks.
 */
public class LocalPartitionReader extends BaseJobInput
    implements SSTableReader, SequenceReader, JobInput {

    // the path this file was opened with.
    private String path = null;

    // ALL chunks we have for this reader.
    private List<DefaultChunkReader> chunkReaders = new ArrayList<DefaultChunkReader>();

    // iterator to handle 
    private Iterator<DefaultChunkReader> iterator = null;

    // the current chunk reader we are on.
    private DefaultChunkReader chunkReader = null;

    // true when we have another record to read
    private boolean hasNext = false;

    // the partition we're reading from.
    private Partition partition;

    // the current chunk we are working with.
    private ChunkReference chunkRef;

    // the count of all records across all chunks for the given file.
    private int count = 0;

    // the index of data blocks for seekTo and scan support.
    protected TreeMap<StructReader,DataBlockReference> dataBlockLookup = null;

    // the current key we are working with.
    private StructReader key = null;

    // the current value we are working with
    private StructReader value = null;

    // the first key in the stream of keys.  Used for scan operation when a
    // scan isn't given a start key.
    private StructReader firstKey = null;

    // by default we just accumulate region metrics in one main metric system.
    // in practice what we want to do is create this externally and pass it in.
    // Providing a default allows us to use the null object pattern and simply
    // call the methods against the metrics even though we don't care about the
    // results since they don't mean much globally. The per-reader version
    // does make it easy to work with in testing though.
    private RegionMetrics regionMetrics = new RegionMetrics();

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
        for( DefaultChunkReader reader : chunkReaders ) {
            count += reader.count();
            reader.setRegionMetrics(regionMetrics);
        }

        dataBlockLookup = buildDataBlockIndex();
        
    }

    private TreeMap<StructReader,DataBlockReference> buildDataBlockIndex() {

        TreeMap<StructReader,DataBlockReference> result = new TreeMap( new StrictStructReaderComparator() );

        // build the index of the ChunkReaders.

        byte[] lastKey = null;


        DataBlockReference last = null;

        for ( int idx = 0; idx < chunkReaders.size(); ++idx ) {

            DefaultChunkReader current = chunkReaders.get( idx );
            
            for( DataBlock db : current.getDataBlocks() ) {

                if ( firstKey == null )
                    firstKey = StructReaders.wrap(db.getFirstKey());

                DataBlockReference ref = new DataBlockReference();
                ref.reader = current;
                ref.idx = idx;
                ref.dataBlock = db;

                result.put( StructReaders.wrap( db.firstKey ), ref );

                // update the linked list structure
                if ( last != null ) {
                    last.next = ref;
                }

                last = ref;

            }

            if ( current.getTrailer().getCount() > 0 ) {
                lastKey = current.getFileInfo().getLastKey();
            }
            
        }

        // this is a fake entry for the last key so that keys that are clearly
        // past the index don't even show up in searches.
        if ( lastKey != null ) {
        
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

        }

        return result;
                    
    }

    /**
     * Find the data block reference that could potentially hold the given key
     * so that I can find it within the given DefaultChunkReader.
     */
    protected DataBlockReference findDataBlockReference( StructReader key ) {
        
        Map.Entry<StructReader,DataBlockReference> entry = dataBlockLookup.floorEntry(key);

        if ( entry == null )
            return null;

        return entry.getValue();

    }

    /**
     * Get the first key in the stream of key/value pairs.
     */
    @Override
    public StructReader getFirstKey() {
        return firstKey;
    }

    @Override
    public Record seekTo( BackendRequest request ) throws IOException {

        List<BackendRequest> requests = new ArrayList();
        requests.add( request );

        LastRecordListener lastRecordListener = new LastRecordListener( null );

        seekTo( requests, lastRecordListener );

        if ( lastRecordListener.getLastRecord() != null ) {
            return lastRecordListener.getLastRecord();
        }

        return null;
        
    }

    @Override
    public boolean seekTo( List<BackendRequest> requests, RecordListener listener ) throws IOException {

        this.key = null;
        this.value = null;

        // the list must be sorted before we service it so that we can
        // access keys on the same block without going backwards and
        // accessing the same block again.  A seek is involved with fetching
        // a block as is optionally decompression and that's expensive.  By
        // first sorting the set we avoid both expensive operations.
        Collections.sort( requests );

        Map<DataBlockReference,List<BackendRequest>> dataBlockLookup =
                new TreeMap<DataBlockReference, List<BackendRequest>>();

        LastRecordListener lastRecordListener = new LastRecordListener( listener );
        
        for( BackendRequest request : requests ) {

            //FIXME skip suspended clients...

            if ( request instanceof ScanBackendRequest ) {

                ScanBackendRequest scanBackendRequest = (ScanBackendRequest)request;

                // We have to set the seek key as the first entry in this SSTable
                // so that we can seekTo it similar to the way GetBackendRequest
                // works.
                if ( scanBackendRequest.getSeekKey() == null ) {
                    StructReader firstKey = getFirstKey();
                    scanBackendRequest.setImplicitStartKey( firstKey );
                }

            }

            StructReader key = request.getSeekKey();

            DataBlockReference ref = findDataBlockReference( key );

            // this key isn't indexed so no need to check.
            if ( ref == null ) {
                request.setComplete(true);
                continue;
            }

            List<BackendRequest> requestsForReference = dataBlockLookup.get( ref );

            if ( requestsForReference == null ) {
                requestsForReference = new ArrayList();
                dataBlockLookup.put(ref, requestsForReference);
            }

            requestsForReference.add( request );
            
        }

        // keep the references we need to page through.
        List<DataBlockReference> dataBlockReferences = new ArrayList<DataBlockReference>();
        dataBlockReferences.addAll( dataBlockLookup.keySet() );

        while( dataBlockReferences.size() > 0 ) {

            DataBlockReference ref = dataBlockReferences.remove(0);

            chunkReader = ref.reader;

            // set the iterator to ALL chunkReader including the current one and
            // continue forward.
            iterator = chunkReaders.subList( ref.idx, chunkReaders.size() ).iterator();

            // update the chunk ref that we're working with.
            chunkRef = new ChunkReference( partition, ref.idx );

            List<BackendRequest> refKeys = dataBlockLookup.get( ref );

            List<BackendRequest> partialScanRequests =
                    ref.reader.seekTo( refKeys, ref.dataBlock, lastRecordListener );

            if ( ref.next != null ) {

                if ( partialScanRequests.size() > 0 ) {

                    // scan requests need to index multiple blocks

                    refKeys = dataBlockLookup.get( ref.next );

                    if ( refKeys != null ) {
                        refKeys.addAll( partialScanRequests );
                        Collections.sort( refKeys );
                    } else {
                        dataBlockLookup.put( ref.next, partialScanRequests );
                        dataBlockReferences.add( 0, ref.next );
                    }

                }

            } else  {

                // this is the LAST block in the entire index and by definition there
                // is nothing left to index.

                for( BackendRequest request : partialScanRequests ) {
                    request.setComplete(true);
                }

            }

        }

        if ( lastRecordListener.getLastRecord() != null ) {

            Record last = lastRecordListener.getLastRecord();
            
            this.key = last.getKey();
            this.value = last.getValue();

            return true;
            
        } else {
            return false;
        }

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

        this.key = chunkReader.key();
        this.value = chunkReader.value();
        
    }
    
    @Override
    public StructReader key() throws IOException {
        return key;
    }

    @Override
    public StructReader value() throws IOException {
        return value;
    }

    @Override
    public void close() throws IOException {

        //TODO: migrate to using IdempotentCloser ...
        
        if ( chunkReader != null ) {
            fireOnChunkEnd( chunkRef );
        }

        new Closer( chunkReaders ).close();
        
    }

    public RegionMetrics getRegionMetrics() {
        return regionMetrics;
    }

    public void setRegionMetrics(RegionMetrics regionMetrics) {
        this.regionMetrics = regionMetrics;
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
    class DataBlockReference implements Comparable<DataBlockReference> {

        StrictStructReaderComparator comparator = new StrictStructReaderComparator();

        // keeps a pointer to the chunk this data block is stored 
        protected DefaultChunkReader reader = null;

        // the position in the main chunk reader list so that we can call
        // subList and get a new iterator for this and all subsequent chunks.
        protected int idx = -1;

        // the data block for this reference.  Used so that we can call seekTo
        // within the actual chunk.
        protected DataBlock dataBlock = null;

        // the next data block reference. We keep a forward linked list so that
        // we can jump to the next block if a scan request isn't finished.
        protected DataBlockReference next = null;

        public int compareTo( DataBlockReference db ) {

            StructReader key0 = StructReaders.wrap( dataBlock.getFirstKey() );
            StructReader key1 = StructReaders.wrap( db.dataBlock.getFirstKey() );

            return comparator.compare( key0, key1 );

        }
        
    }
    
}
