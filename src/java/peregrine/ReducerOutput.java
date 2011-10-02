package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.shuffle.*;
import peregrine.io.*;

public interface ReducerOutput {

    public void emit( byte[] key , byte[] value );
    
}