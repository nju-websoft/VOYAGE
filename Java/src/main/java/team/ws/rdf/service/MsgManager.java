package team.ws.rdf.service;

import cl.uchile.dcc.blabel.label.GraphColouring;
import com.google.common.hash.HashCode;
import org.apache.commons.text.StringEscapeUtils;
import org.jgrapht.Graph;
import org.jgrapht.alg.connectivity.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;
import team.ws.rdf.dao.RDFDao;
import team.ws.rdf.model.MsgData;
import team.ws.rdf.model.MyTerm;
import team.ws.rdf.model.MyTriple;

import java.io.*;
import java.util.*;

import static cl.uchile.dcc.blabel.modification.LeanLabeler.leanAndLabel;


public class MsgManager {

    public static String getTermStringNtriples(MyTerm mt) {
        if (mt.isIri()) {
            return  "<" + mt.getIri() + ">";
        } else if (mt.isBlankNode()) {
            return  "_:" + mt.getIri();
        } else if (mt.isLiteral()) {
            return "\"" + StringEscapeUtils.escapeJava(mt.getIri()) + "\""; // Avoid single backslash \
        }
        return null;
    }

    public static String getTripleStringForHash(MyTriple mt, MyTerm[] myTerms) {
        int sub_id = mt.getSubject_id(), pred_id = mt.getPredicate_id(), obj_id = mt.getObject_id();
        MyTerm subject = myTerms[sub_id], predicate = myTerms[pred_id], object = myTerms[obj_id];
        String subString = getTermStringNtriples(subject),
                predString = getTermStringNtriples(predicate),
                objString = getTermStringNtriples(object);
        return subString + " " + predString + " " + objString + " .";
    }

    public static List<MyTriple> computeHashRefactoring(MsgData msgData) throws IOException, InterruptedException, GraphColouring.HashCollisionException, IllegalArgumentException {
        Set<MyTriple> allTriple = msgData.allTriple;
        Map<Integer, Set<MyTriple>> bnode2triple = msgData.bnode2triple;
        MyTerm[] myTerms = msgData.myTerms;

        Graph<Integer, DefaultEdge> graph = new SimpleGraph<>(DefaultEdge.class);
        Set<String> triples;

        for (MyTriple init: allTriple) {
            int subject_id = init.getSubject_id(), predicate_id = init.getPredicate_id(), object_id = init.getObject_id();
            boolean flag1 = myTerms[subject_id].isBlankNode(), flag2 = myTerms[object_id].isBlankNode();
            // only blank nodes can be vertexes on graph
            if (flag1) {
                graph.addVertex(subject_id);
            }
            if (flag2) {
                graph.addVertex(object_id);
            }
            // both subject and object are blank nodes there is an edge on graph
            if (flag1 && flag2) {
                graph.addEdge(subject_id, object_id);
            }
            //  both subject and object are NOT blank nodes (i.e. both are IRIs) can be computed hash immediately
            if (!flag1 && !flag2) {
                triples = new HashSet<>();
                triples.add(getTripleStringForHash(init, myTerms));
                HashCode hash = leanAndLabel(triples);
                init.setHash(String.valueOf(hash));
            }
        }

        ConnectivityInspector<Integer, DefaultEdge> connectivityInspector = new ConnectivityInspector<>(graph);
        List<Set<Integer>> weaklyConnectedSet = connectivityInspector.connectedSets();
        for (Set<Integer> connectedSet: weaklyConnectedSet) {
            Set<MyTriple> msg = new HashSet<>();
            for (Integer node: connectedSet) {
                msg.addAll(bnode2triple.getOrDefault(node, new HashSet<>()));
            }
            triples = new HashSet<>();
            for (MyTriple mt: msg) {
                triples.add(getTripleStringForHash(mt, myTerms));
            }
            HashCode hash = leanAndLabel(triples);
            for (MyTriple mt: msg) {
                mt.setHash(String.valueOf(hash));
            }
        }
        return new ArrayList<>(allTriple);
    }

    public static void hashTriplesByFileId(RDFDao dao, int dataset_id, int file_id) throws IOException, InterruptedException, GraphColouring.HashCollisionException, IllegalArgumentException {
        MsgData msgData = dao.getMsgDataByFileIdStream(file_id);
        List<MyTriple> myTripleList = computeHashRefactoring(msgData);
        dao.insertMsgCodeTriple(dataset_id, file_id, myTripleList);
    }


}
