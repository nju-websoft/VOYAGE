package team.ws.rdf.service;
import org.apache.jena.ext.com.google.common.collect.BiMap;
import org.apache.jena.ext.com.google.common.collect.HashBiMap;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.impl.Util;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.util.SplitIRI;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.log4j.Logger;
import team.ws.rdf.model.MyTerm;
import team.ws.rdf.model.MyTriple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

public class MyStreamRDF implements StreamRDF {
    private int baseTermId;
    private boolean hasRDFList;
    private String RDFList;

    // IRI, blank node, literal：0, 1, 2
    private final int IRI = 0;
    private final int BLANKNODE = 1;
    private final int LITERAL = 2;

    private BiMap<MyTerm, Integer> myTermIntegerBiMap;
    private BiMap<Integer, MyTerm> integerMyTermBiMap;

    private ArrayList<MyTriple> myTriples;

    public MyStreamRDF(int baseTermId) {
        this.baseTermId = baseTermId;
    }

    @Override
    public void start() {
        this.myTermIntegerBiMap = HashBiMap.create();
        this.integerMyTermBiMap = myTermIntegerBiMap.inverse();

        this.myTriples = new ArrayList<>();
        this.hasRDFList = false;
        this.RDFList = null;
    }

    private MyTerm addTerm(String iri, String label, int kind) {
        int term_id = this.baseTermId + myTermIntegerBiMap.size();
        MyTerm term = new MyTerm(iri, label, kind, term_id);
        if (myTermIntegerBiMap.containsKey(term)) {
            term_id = myTermIntegerBiMap.get(term);
        } else {
            myTermIntegerBiMap.put(term, term_id);
        }
        return integerMyTermBiMap.get(term_id);
    }

    private void kindReasoner(MyTerm subj, MyTerm pred, MyTerm obj) {
        String subLabel = subj.getLabel(), preLabel = pred.getLabel(), objLabel = obj.getLabel();
        String subIRI = subj.getIri(), preIRI = pred.getIri(), objIRI = obj.getIri();
        int subKind = subj.getKind(), preKind = pred.getKind(), objKind = obj.getKind();

        // kind reasoner, source = vocabulary + ontology + dataset
        // may need to check
        if (this.hasRDFList && this.RDFList.equals(subLabel)) {
            if (preIRI.equals(RDF.first.toString())) {
                obj.setClass(true);
            } else if (preIRI.equals(RDF.rest.toString())) {
                if (objIRI.equals(RDF.nil.toString())) {
                    this.hasRDFList = false;
                    this.RDFList = null;
                } else {
                    this.RDFList = objLabel;
                }
            }
        }

        if (preIRI.equals(RDF.type.toString())) {
            /* Typing */
            if (objKind == IRI) {  // object is IRI
                if (objIRI.equals(RDFS.Class.toString()) || objIRI.equals(RDFS.Datatype.toString())
                        || objIRI.equals(OWL.Class.toString()) || objIRI.equals(OWL.DataRange.toString())
                        || objIRI.equals(OWL.DeprecatedClass.toString())
                        || objIRI.equals(OWL.Restriction.toString())) {
                    /* subj -> class */
                    subj.setClass(true);
                } else if (objIRI.equals(RDF.Property.toString())
                        || objIRI.equals(RDFS.ContainerMembershipProperty.toString())
                        || objIRI.equals(OWL.AnnotationProperty.toString())
                        || objIRI.equals(OWL.DatatypeProperty.toString())
                        || objIRI.equals(OWL.DeprecatedProperty.toString())
                        || objIRI.equals(OWL.FunctionalProperty.toString())
                        || objIRI.equals(OWL.InverseFunctionalProperty.toString())
                        || objIRI.equals(OWL.ObjectProperty.toString())
                        || objIRI.equals(OWL.OntologyProperty.toString())
                        || objIRI.equals(OWL.SymmetricProperty.toString())
                        || objIRI.equals(OWL.TransitiveProperty.toString())) {
                    /* subj -> property */
                    subj.setProperty(true);
                } else if (objIRI.equals(OWL.Ontology.toString())) {
                    /* subj -> ontology */
                    subj.setSource(true);
                }
            }
            /* obj -> class */
            obj.setClass(true);
        } else if (preIRI.equals(OWL.cardinality.toString())
                || preIRI.equals(OWL.hasValue.toString())
                || preIRI.equals(OWL.maxCardinality.toString())
                || preIRI.equals(OWL.minCardinality.toString())
                || preIRI.equals(OWL.oneOf.toString())) {
            /* subj -> class */
            subj.setClass(true);
        } else if (preIRI.equals(RDFS.subClassOf.toString())
                || preIRI.equals(OWL.allValuesFrom.toString())
                || preIRI.equals(OWL.complementOf.toString())
                || preIRI.equals(OWL.disjointWith.toString())
                || preIRI.equals(OWL.equivalentClass.toString())
                || preIRI.equals(OWL.someValuesFrom.toString())) {
            /* subj -> class, obj -> class */
            subj.setClass(true);
            obj.setClass(true);
        } else if (preIRI.equals(OWL.intersectionOf.toString())
                || preIRI.equals(OWL.unionOf.toString())) {
            /* subj -> class, obj -> class list */
            subj.setClass(true);
            // need to check
            this.hasRDFList = true;
            this.RDFList = objIRI;
        } else if (preIRI.equals(OWL.onProperty.toString())) {
            /* subj -> class, obj -> property */
            subj.setClass(true);
            obj.setProperty(true);
        } else if (preIRI.equals(RDFS.subPropertyOf.toString())
                || preIRI.equals(OWL.equivalentProperty.toString())
                || preIRI.equals(OWL.inverseOf.toString())) {
            /* subj -> property, obj -> property */
            subj.setProperty(true);
            obj.setProperty(true);
        } else if (preIRI.equals(RDFS.domain.toString()) || preIRI.equals(RDFS.range.toString())) {
            /* subj -> property, obj -> class */
            subj.setProperty(true);
            obj.setClass(true);
        } else if (preIRI.equals(OWL.backwardCompatibleWith.toString())
                || preIRI.equals(OWL.imports.toString())
                || preIRI.equals(OWL.incompatibleWith.toString())
                || preIRI.equals(OWL.priorVersion.toString())) {
            /* subj -> ontology, obj -> ontology */
            subj.setSource(true);
            obj.setSource(true);
        } else if (preIRI.equals(RDFS.isDefinedBy.toString()) || preIRI.equals(RDFS.seeAlso.toString())) {
            obj.setSource(true);
        } else if (preIRI.equals("http://purl.org/dc/dcam/memberOf")) {
            obj.setSource(true);
        } else if (preIRI.equals("http://purl.org/vocab/vann/preferredNamespaceUri")
                || preIRI.equals("http://purl.org/vocab/vann/preferredNamespacePrefix")) {
            subj.setSource(true);
        }

        /* pred -> property */
        pred.setProperty(true);
    }

    private String getIriLocalName(String iri) {
        return SplitIRI.localname(iri);
    }

    @Override
    public void triple(Triple triple) {
        Node subject = triple.getSubject();
        Node predicate = triple.getPredicate();
        Node object = triple.getObject();

        String subIRI = subject.toString();
        String preIRI = predicate.toString();
        String objIRI = object.toString(true);

        String subLabel, preLabel, objLabel;
        int subKind, preKind, objKind;

        // IRI, blank node, literal：0, 1, 2
        // the subject, which is an IRI or a blank node
        // the predicate, which is an IRI
        // the object, which is an IRI, a literal or a blank node
        if (subject.isBlank()) {  // blank node
            subKind = BLANKNODE;
            subLabel = null;
        } else {  // IRI
            subKind = IRI;
//            subLabel = subject.getLocalName();
            subLabel = getIriLocalName(subIRI);
        }
        if (preIRI.equals(RDFS.label.toString())) {  // if <sub rdfs:label obj> then subLabel=obj
            subLabel = objIRI;
        }

        preKind = IRI;
//        preLabel = predicate.getLocalName();
        preLabel = getIriLocalName(preIRI);

        if (object.isBlank()) {  // blank node
            objKind = BLANKNODE;
            objLabel = null;
        } else if (object.isLiteral()) {  // literal
            objKind = LITERAL;
            objLabel = object.getLiteralLexicalForm();
        } else {  // IRI
            objKind = IRI;
//            objLabel = object.getLocalName();
            objLabel = getIriLocalName(objIRI);
        }

        MyTerm subj = addTerm(subIRI, subLabel, subKind);
        MyTerm pred = addTerm(preIRI, preLabel, preKind);
        MyTerm obj = addTerm(objIRI, objLabel, objKind);
        MyTerm dt = null;  // if object is literal, dt != null

        // kind reasoner, including the following if statement
        kindReasoner(subj, pred, obj);
        /* obj's datatype -> class */
        if (objKind == LITERAL) {  // object is literal
            String datatypeURI = object.getLiteralDatatypeURI();
            if (datatypeURI != null) {
                dt = addTerm(datatypeURI, datatypeURI.substring(Util.splitNamespaceXML(datatypeURI)), 0);
                dt.setClass(true);
                dt.addCount();
            }
        }

        // set EDP
        if (subj.isEntity()) {
            if (preIRI.equals(RDF.type.toString())) {
                subj.addClass(obj.getTerm_id());
            } else {
                subj.addForwardProperty(pred.getTerm_id());
            }
        }
        if (obj.isEntity()) {
            obj.addBackwardProperty(pred.getTerm_id());
        }

        subj.addCount();
        pred.addCount();
        obj.addCount();

        this.myTriples.add(new MyTriple(subj.getTerm_id(), pred.getTerm_id(), obj.getTerm_id()));

        System.out.println(subIRI + " " + preIRI + " " + objIRI);
    }

    @Override
    public void quad(Quad quad) {

    }

    @Override
    public void base(String s) {

    }

    @Override
    public void prefix(String s, String s1) {

    }

    public ArrayList<MyTriple> getMyTriples() {
        return myTriples;
    }

    public Collection<MyTerm> getMyTerms() {
        return myTermIntegerBiMap.keySet();
    }

    public int getBaseTermId() {
        return baseTermId + integerMyTermBiMap.size();
    }

    @Override
    public void finish() {
    }
}
