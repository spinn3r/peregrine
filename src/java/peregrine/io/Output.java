package peregrine.io;

import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;

public final class Output {

    private List<OutputReference> references = new ArrayList();

    public Output( String[] paths ) {
        for( String path : paths ) {
            add( new FileOutputReference( path ) );
        }
    }

    public void add( OutputReference ref ) {
        this.references.add( ref );
    }

    public List<OutputReference> getReferences() {
        return references;
    }
    
}