package peregrine;

import java.io.*;
import java.util.*;

public interface Key {

    public byte[] toBytes();

    public void fromBytes( byte[] data );

}