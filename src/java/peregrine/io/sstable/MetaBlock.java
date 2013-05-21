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

import org.jboss.netty.buffer.ChannelBuffer;
import peregrine.util.netty.ChannelBufferWritable;
import peregrine.util.netty.StreamReader;

import java.io.IOException;

public class MetaBlock extends BaseBlock {

    @Override
    public void read( ChannelBuffer buff ) {

        super.read( buff );

        StreamReader reader = new StreamReader( buff );

    }

    @Override
    public void write( ChannelBufferWritable writer ) throws IOException {
        super.write( writer );
    }

}