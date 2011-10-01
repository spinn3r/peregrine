package maprunner;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.shuffle.*;
import maprunner.io.*;

public interface ReducerOutput {

    public void emit( byte[] key , byte[] value );
    
}