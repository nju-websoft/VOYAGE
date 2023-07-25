package team.ws.rdf.model;


import java.util.*;

public class EDPIRI extends EDP{
    public HashSet<String> classes;
    public HashSet<String> forwardProperties;
    public HashSet<String> backwardProperties;
    public ArrayList<int[]> originalIdList;

    public EDPIRI() {
        classes = new HashSet<>();
        forwardProperties = new HashSet<>();
        backwardProperties = new HashSet<>();
        originalIdList = new ArrayList<>();
    }

    public void addIRI(String iri, int kind) {
        if (kind == CLASS) {
            classes.add(iri);
        } else if (kind == FORWARDPROPERTY) {
            forwardProperties.add(iri);
        } else if (kind == BACKPROPERTY) {
            backwardProperties.add(iri);
        }
    }

    public void addOriginalId(int dataset_id, int file_id, int edp_id) {
        originalIdList.add(new int[]{dataset_id, file_id, edp_id});
    }

    public ArrayList<int[]> getOriginalIdList() {
        return originalIdList;
    }

    public HashSet<String> getClasses() {
        return classes;
    }

    public HashSet<String> getForwardProperties() {
        return forwardProperties;
    }

    public HashSet<String> getBackwardProperties() {
        return backwardProperties;
    }

    public void mergeOriginalIdList(EDPIRI other) {
        originalIdList.addAll(other.getOriginalIdList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EDPIRI edpiri = (EDPIRI) o;
        return Objects.equals(classes, edpiri.classes) && Objects.equals(forwardProperties, edpiri.forwardProperties) && Objects.equals(backwardProperties, edpiri.backwardProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classes, forwardProperties, backwardProperties);
    }

    @Override
    public String toString() {
        StringBuilder temp = new StringBuilder("[");
        for (int[] ori : originalIdList) {
            temp.append(Arrays.toString(ori));
        }
        temp.append("]");
        return "EDPIRI{" +
                "classes=" + classes +
                ", forwardProperties=" + forwardProperties +
                ", backwardProperties=" + backwardProperties +
                ", originalIdList=" +  temp +
                '}';
    }
}
