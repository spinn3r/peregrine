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
package peregrine.shuffle;

public class ShuffleHeader {

    /**
     * The partition id for this header.
     */
    public int partition;

    /**
     * The position in this file where the data starts.
     */
    public int offset;

    /**
     * The number of packets for this partition in this shuffle file.
     */
    public int nr_packets;

    /**
     * The total number of key/value pairs.
     */
    public int count;

    /**
     * The length in bytes of this data.
     */
    public int length;

    public int getOffset() {
        return offset;
    }
    
    public String toString() {
        
        return String.format( "partition: %s, offset: %,d, nr_packets: %,d, count: %,d, length: %,d" ,
                              partition, offset, nr_packets, count, length );
        
    }
    
}
