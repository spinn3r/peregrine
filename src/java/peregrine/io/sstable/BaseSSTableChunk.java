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

package peregrine.io.sstable;

import java.util.ArrayList;
import java.util.List;

/**
 * Methods common to all SSTable chunk readers and writers.
 */
public abstract class BaseSSTableChunk {

    // information about the file we are writing to...
    protected FileInfo fileInfo = new FileInfo();

    // trailer information for the file
    protected Trailer trailer = new Trailer();

    // list of data blocks in the index.
    protected List<DataBlock> dataBlocks = new ArrayList<DataBlock>();

    // list of meta blocks in the index
    protected List<MetaBlock> metaBlocks = new ArrayList<MetaBlock>();

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