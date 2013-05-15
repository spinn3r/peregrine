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
package peregrine.shuffle.receiver;

import java.util.concurrent.*;
import peregrine.shuffle.*;

public class ShuffleReceiverFlushCallable implements Callable {

    private ShuffleOutputWriter writer;
    
    ShuffleReceiverFlushCallable( ShuffleOutputWriter writer ) {

        if ( writer == null )
            throw new NullPointerException( "writer" );
        
        this.writer = writer;
    }

    @Override
    public Object call() throws Exception {

        // close this in a background task since this blocks.
        writer.close();
        
        return null;
        
    }
    
}
