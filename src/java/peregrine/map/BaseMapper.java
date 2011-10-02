package peregrine.map;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.shuffle.*;
import peregrine.io.*;

public abstract class BaseMapper {

    public int partitions = 0;

    private MapperOutput stdout = null;
    
    public void init( MapperOutput... output ) {

        if ( output.length > 0 )
            this.stdout = output[0];

    }

    public void cleanup() { }

    public final void emit( byte[] key,
                            byte[] value ) {

        stdout.emit( key, value );
        
    }

}

