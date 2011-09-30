package maprunner.io;

import java.util.*;

import maprunner.*;
import maprunner.util.*;
import maprunner.keys.*;

public final class Input {

    private List<InputReference> references = new ArrayList();
    
    public void add( InputReference ref ) {
        this.references.add( ref );
    }

    public List<InputReference> getReferences() {
        return references;
    }
    
}