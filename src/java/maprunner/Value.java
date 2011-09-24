package maprunner;

import java.io.*;
import java.util.*;

public interface Value {

    public byte[] toBytes();

    public void fromBytes( byte[] data );

}