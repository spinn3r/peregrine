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
package peregrine.sysstat;

import java.io.*;
import java.util.*;
import java.math.*;

import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

/**
 */
public class InterfaceStat extends BaseStat implements Diffable<InterfaceStat> {
    
    BigDecimal readBits     = new BigDecimal( 0 );
    BigDecimal writtenBits  = new BigDecimal( 0 );

    @Override
    public InterfaceStat diff( InterfaceStat after ) {

        InterfaceStat result = new InterfaceStat();
        
        result.name = name;
        result.duration = after.timestamp - timestamp;

        result.readBits    = after.readBits.subtract( readBits );
        result.writtenBits = after.writtenBits.subtract( writtenBits );

        return result;
        
    }

    /**
     * Compute the rate of this state over the given interval.
     */
    @Override
    public InterfaceStat rate( long interval ) {

        InterfaceStat result = new InterfaceStat();

        result.name = name;
        result.duration = interval;

        result.readBits = overInterval( readBits, interval );
        result.writtenBits = overInterval( writtenBits, interval );

        return result;
        
    }

    @Override
    public String toString() {

        StringBuilder buff = new StringBuilder();

        buff.append( format( name, readBits.longValue(), writtenBits.longValue() ) );

        return buff.toString();

    }
    
}

