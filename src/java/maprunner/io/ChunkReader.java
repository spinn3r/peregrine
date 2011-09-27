package maprunner.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import maprunner.*;
import maprunner.util.*;
import maprunner.keys.*;
import maprunner.values.*;

public interface ChunkReader {

    public Tuple read() throws IOException;

}