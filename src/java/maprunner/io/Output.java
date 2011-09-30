package maprunner.io;

import java.util.*;

import maprunner.*;
import maprunner.util.*;
import maprunner.keys.*;

public final class Output {

    private List<OutputReference> references = new ArrayList();
    
    public void add( OutputReference ref ) {
        this.references.add( ref );
    }

    public List<OutputReference> getReferences() {
        return references;
    }
    
}