import { RDF, TREE, turtleStringToStore } from "@treecg/versionawareldesinldp";
import { Quad, Quad_Subject, Store, DataFactory, NamedNode } from "n3";
const namedNode = DataFactory.namedNode;
const literal = DataFactory.literal;
import { Resource } from "./EventSource";
import { existsSync, readFileSync } from "fs";

/**
 * Returns all the triples as a store from a file at a given filepath
 *  note: This method can throw depending on the file(path)
 * @param filepath
 * @returns {Promise<Store>}
 */
export async function storeFromFile(filepath: string): Promise<Store> {
    if (!existsSync(filepath)){
        throw Error("The filepath is invalid.");
    }
    // if the file content is invalid, the method below will throw a
    // different error
    return await turtleStringToStore(readFileSync(filepath, "utf-8"));
}

/**
 * Extracts all resources (along referenced data) containing the given treePath as a predicate
 * from the given store, and adds relevant tree:member data.
 * 
 * @param store The store containing all triples (as well as relevant referenced data)
 * @param treePath Predicate which every "main" subject contains (e.g. "http://purl.org/dc/terms/created")
 * @param eventStreamURI Complete name of the stream where individual resources gets added to (e.g. http://localhost:3000/#EventStream)
 * @returns {Resource[]}
 */
 export function extractResources(
    store: Store,
    treePath: string,
    eventStreamURI: string
): Resource[] {
    // extract every resource based on the subject, where the subject has the predicate
    // treePath
    let mainSubjects: Quad_Subject[] | Set<string> = store.getSubjects(
        treePath, null, null
    );
    const resources = mainSubjects.map(subject => {
        // extract triples based on subject
        const resource = store.getQuads(subject, null, null, null) as Resource;
        // add tree:member
        resource.push(new Quad(namedNode(eventStreamURI), namedNode(TREE.member), subject));
        return resource;
    });
    mainSubjects = new Set(mainSubjects.map(subj => subj.id));
    // it's possible for any of resource's object values to be an object further defined here,
    // if that is the case they get added to this resource
    for (const quads of resources) {
        // to avoid issues with data referencing themselves in a circle, duplicates are filtered
        // out (alongside the current subject)
        const existingObjects = new Set<string>();
        existingObjects.add(quads[0].subject.id);
        for (const quad of quads) {
            if (existingObjects.has(quad.object.id)) {
                continue;
            }
            existingObjects.add(quad.object.id);
            // all quads with subjects equal to its object representation gets added to this
            // resource entry, so the original subjects' data is completely present inside this
            // single collection of resources
            // this approach already works recursively, as push adds new elements to the end
            // quads having another main resource (that is not the current resource) as object
            // are getting filtered out as well, as they are not further defined
            quads.push(
                ...store.getQuads(quad.object, null, null, null).filter((obj) => {
                    return obj.object.id === quads[0].subject.id || !((mainSubjects as Set<string>).has(obj.object.id))
                })
            );
        }
    }
    return resources;
}

/**
 * Generates a shape representation (collection of `Quad`s, `Resource`) of
 * the given resource
 *  note: This method has to be used on a single unit resource (not a
 *  collection/batch of unrelated units)
 * @param resource The single resource (as well as its properties, as obtained
 *  from `extractResources`)
 * @param eventStreamURL The URL of the LDESinLDP container, used as a prefix for
 *  the shape types
 * @returns {Resource} a collection of `Quad`s (`Resource`) representing the shape
 */
export function generateShape(resource: Resource, eventStreamURL: string): Resource {
    // TODO: maybe create a generateShape(Resource[] ...) method which would use most extreme
    // properties automatically for min and maxCount, and feed it to this method
    const shape : Quad[] = [];
    const subjects = new Map<string, NamedNode>();
    const sh = "http://www.w3.org/ns/shacl#";
    const nodeKinds = {
        "Literal": sh + "Literal",
        "Variable": sh + "Literal",
        "BlankNode": sh + "BlankNodeOrIRI",
        "NamedNode": sh + "IRI"
    };
    const resourceStore = new Store(resource);
    const extractValue = (val: string) => ( val.match(/.+[\/#]([a-zA-Z0-9_\-]*)$/)![1] );
    // all named nodes (with type) gets added to the store first
    for (const quad of resourceStore.getQuads(null, RDF.type, null, null)) {
        // every unique named node subject gets its own sh:NodeShape
        const subj = namedNode(eventStreamURL + extractValue(quad.object.value) + 'Shape');
        shape.push(new Quad(
            subj, namedNode(RDF.type), namedNode(sh + "NodeShape")
        ));
        // adding its type as sh:targetClass
        shape.push(new Quad(
            subj, namedNode(sh + "targetClass"), quad.object
        ));
        // mark as processed by caching this subj
        subjects.set(quad.subject.id, subj);
    }
    // all other properties found in the resource now gets added
    for (const quad of resource) {
        // all properties (except for rdf:type) map to a sh:property
        if (quad.predicate.id === RDF.type) {
            continue;
        }
        const subj = subjects.get(quad.subject.id);
        if (!subj) {
            // no type known, skipping
            // message below can be enabled to debug missing values
            // console.log(`Skipping ${quad.subject.value} for shape generation`);
            continue;
        }
        const obj = namedNode(eventStreamURL + extractValue(quad.predicate.value) + "Property");
        shape.push(new Quad(subj, namedNode(sh + "property"), obj));
        shape.push(new Quad(obj, namedNode(RDF.type), namedNode(sh + "propertyShape")));
        shape.push(new Quad(obj, namedNode(sh + "path"), quad.predicate));
        shape.push(new Quad(obj, namedNode(sh + "nodeKind"), namedNode(nodeKinds[quad.object.termType])));
        shape.push(new Quad(obj, namedNode(sh + "minCount"), literal(1)));
        // max count is equal to the number of objects matching this exact query
        shape.push(new Quad(obj, namedNode(sh + "maxCount"), literal(
            resourceStore.getObjects(quad.subject, quad.predicate, null).length
        )));
        let objNode;
        if (objNode = subjects.get(quad.object.id)) {
            // add a relation to its quad.object.id shape node name
            // this can throw if rdf:type was not defined, but this is by design
            // as the resource is considered incomplete in that case
            shape.push(new Quad(obj, namedNode(sh + "node"), objNode));
        }
    }
    return shape;
}

/**
 * Batches resources together, reducing the size of the top level array (array of resources)
 * by increasing the size of the bottom level array (array of quads in a single resource)
 * It is important that resources are already properly sorted (if relevant) prior to batching
 * as the order is preserved
 * 
 * @param source original collection of resources
 * @param count number of resources that should be grouped together
 * @returns {Resource[]} a (smaller) collection of resources, containing every quad from the
 * original source
 */
 export function batchResources(source: Resource[], count: number): Resource[] {
    if (count < 1) {
        // Invalid, returning the original collection
        return source;
    }
    const resources = Array.from(
        Array(Math.floor(source.length / count) + 1), () => new Array()
    );
    for (const [i, resource] of source.entries()) {
        resources[Math.floor(i / count)].push(...resource);
    }
    if (resources[resources.length - 1].length === 0) {
        // drop the last one if empty (can happen in some scenarios)
        resources.length -= 1;
    }
    return resources;
}
