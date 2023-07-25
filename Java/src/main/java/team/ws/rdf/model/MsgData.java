package team.ws.rdf.model;

import java.util.Map;
import java.util.Set;

public class MsgData {
    public Set<MyTriple> allTriple;
    public Map<Integer, Set<MyTriple>> bnode2triple;
    public MyTerm[] myTerms;

    public MsgData(Set<MyTriple> allTriple, Map<Integer, Set<MyTriple>> bnode2triple, MyTerm[] myTerms) {
        this.allTriple = allTriple;
        this.bnode2triple = bnode2triple;
        this.myTerms = myTerms;
    }
}
