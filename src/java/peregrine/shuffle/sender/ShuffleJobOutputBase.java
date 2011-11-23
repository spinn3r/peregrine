package peregrine.shuffle.sender;

import java.io.*;
import java.util.concurrent.*;

import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.util.*;
import peregrine.io.chunk.*;
import com.spinn3r.log5j.Logger;

public abstract class ShuffleJobOutputBase implements ShuffleJobOutputDelegate {
    
    protected long length = 0;

    @Override
    public long length() { 
        return this.length;
    }

}

