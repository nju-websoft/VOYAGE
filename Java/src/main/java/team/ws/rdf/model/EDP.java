package team.ws.rdf.model;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Set;

public class EDP {
    public int edp_id;
    public Set<Integer> classes;
    public Set<Integer> forwardProperties;
    public Set<Integer> backwardProperties;

    // class, forwardproperty, backproperty: 1, 2, 3
    public final static int CLASS = 1;
    public final static int FORWARDPROPERTY = 2;
    public final static int BACKPROPERTY = 3;


    public EDP(int edp_id, Set<Integer> classes, Set<Integer> forwardProperties, Set<Integer> backwardProperties) {
        this.edp_id = edp_id;
        this.classes = classes;
        this.forwardProperties = forwardProperties;
        this.backwardProperties = backwardProperties;
    }

    public EDP() {
    }

    public void setEdp_id(int edp_id) {
        this.edp_id = edp_id;
    }

    public int getEdp_id() {
        return edp_id;
    }

    public ArrayList<int[]> getEdpRows() {
        ArrayList<int[]> edpRows = new ArrayList<>();
        for (Integer cid : classes) {
            edpRows.add(new int[]{edp_id, CLASS, cid});
        }
        for (Integer fid : forwardProperties) {
            edpRows.add(new int[]{edp_id, FORWARDPROPERTY, fid});
        }
        for (Integer bid : backwardProperties) {
            edpRows.add(new int[]{edp_id, BACKPROPERTY, bid});
        }
        return edpRows;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EDP edp = (EDP) o;
        return Objects.equals(classes, edp.classes) && Objects.equals(forwardProperties, edp.forwardProperties) && Objects.equals(backwardProperties, edp.backwardProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classes, forwardProperties, backwardProperties);
    }
}
