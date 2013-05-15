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
package peregrine.os;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

import org.jboss.netty.buffer.*;

import peregrine.http.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.io.util.*;
import peregrine.config.*;

import com.spinn3r.log5j.Logger;

/**
 *
 */
public abstract class BaseMappedFile extends IdempotentCloser {

    protected FileChannel channel;

    protected long offset = 0;

    protected long length = 0;

    protected Closer closer = new Closer();

    protected File file;

    protected int fd;

    protected Config config;

    protected boolean fadviseDontNeedEnabled = false;

    public File getFile() {
        return file;
    }

    public int getFd() throws IOException {

        if ( closer.isClosed() )
            throw new IOException( "closed" );
        
        return fd;
        
    }

    public long length() {
        return length;
    }

    public boolean isClosed() {
        return closer.isClosed();
    }

    @Override
    public String toString() {
        return file.toString();
    }

}
