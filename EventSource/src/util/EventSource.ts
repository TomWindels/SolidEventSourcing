import {
    Communication,
    extractTimestampFromLiteral,
    LDESMetadata,
    LDPCommunication,
    storeToString,
    TREE,
    turtleStringToStore
} from "@treecg/versionawareldesinldp";
import {
    Literal,
    Quad,
    Quad_Object,
    Store,
    Writer,
    DataFactory
} from "n3";
import {
    existsSync,
    readFileSync
} from "fs";
import {Session} from "@rubensworks/solid-client-authn-isomorphic";
import { generateShape } from "./Processing";

const namedNode = DataFactory.namedNode;

// The semantics of Resource is the data point itself (!! not to be confused with an ldp:Resource)
export type Resource = Quad[]
// a dictionary which maps an ldp:containerURL to an array of Resources
export type BucketResources = { [p: string]: Resource[] }

/**
 * @param credentialsFile Filepath to a JSON containing credentials to setup a
 * Solid communication session
 * @returns {Promise<Session | undefined>}
 */
export async function initSession(credentialsFilepath: string): Promise<Session | undefined> {
    if (existsSync(credentialsFilepath)) {
        const credentials = JSON.parse(readFileSync(credentialsFilepath, 'utf-8'));
        const session = new Session();
        await session.login({
            clientId: credentials.clientId,
            clientSecret: credentials.clientSecret,
            refreshToken: credentials.refreshToken,
            oidcIssuer: credentials.issuer,
        });
        return session;
    }
    return undefined;
}

/**
 * Calculates to which bucket (i.e. the ldp:Container) the resource should be added.
 * When the returned url is none, this means the resource its timestamp is less than all current bucket timestamps.
 * @param resource
 * @param metadata
 * @returns {string}
 */
export function calculateBucket(resource: Resource, metadata: LDESMetadata): string {
    const relations = metadata.views[0].relations
    const resourceTs = getTimeStamp(resource, metadata.timestampPath)

    let timestampJustSmaller = 0
    let correspondingUrl = "none";
    for (const relation of relations) {
        const relationTs: number = new Date(relation.value).getTime()
        if (relationTs <= resourceTs && timestampJustSmaller < relationTs) {
            timestampJustSmaller = relationTs
            correspondingUrl = relation.node
        }
    }
    return correspondingUrl;
}

/**
 * The new container URL is calculated based on the container URL where too many resources reside and a timestamp
 * @param containerURL
 * @param timestamp
 */
export function createBucketUrl(containerURL: string, timestamp: number) {
    const split = containerURL.split('/')
    return `${split.slice(0, split.length - 2).join('/')}/${timestamp}/`
}

/**
 * Retrieve timestamp of a resource (ms)
 * @param resource
 * @param timestampPath
 * @returns {number}
 */
export function getTimeStamp(resource: Resource, timestampPath: string): number {
    const resourceStore = new Store(resource)
    return extractTimestampFromLiteral(resourceStore.getObjects(null, timestampPath, null)[0] as Literal)// Note: expecting real xsd:dateTime
}

export async function prefixesFromFilepath(path: string, url?: string): Promise<any> {
    let prefixes: { [key: string]: string } = {};
    if (url) {
        prefixes[""] = url + "#";
    }
    if (existsSync(path)) {
        const store = await turtleStringToStore(readFileSync(path, "utf-8"));
        // only the triples using predicate "<http://purl.org/vocab/vann/preferredNamespacePrefix>"
        // are relevant, as these represent prefix (= object) and URI (= subject)
        const prefixQuads = store.getQuads(null, namedNode("http://purl.org/vocab/vann/preferredNamespacePrefix"), null, null);
        for (const prefixQuad of prefixQuads) {
            if (prefixQuad.object.termType != "Literal" || !/^"[^"]+"$/.test(prefixQuad.object.id)) {
                // the object does not represent a string literal, skipping this entry
                continue;
            }
            prefixes[prefixQuad.object.value] = prefixQuad.subject.value;
        }
    }
    return prefixes;
}

/**
 * Converts a resource (quad array) to an optimised turtle string representation by grouping subjects
 * together, using prefixes wherever possible and replacing blank nodes with their properties.
 * Note: blank nodes referenced to as objects, but not found as subjects in other quads, can cause
 *  issues
 * Note: a more processing performant solution might be possible, by creating a store from the resource
 *  and indexing from there instead of two seperate maps
 *
 * @param resource The resource that gets converted to a string
 * @param _prefixes An object which members are strings, member name being the short prefix and its
 *  value a string representing its URI. Example: `{"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"}`
 * @returns {string}
 */
export function resourceToOptimisedTurtle(resource: Resource, _prefixes: any): string {
    // get a grouped overview of this resource's content
    const named = new Map<string, Map<string, Quad_Object[]>>();
    const blank = new Map<string, Map<string, Quad_Object[]>>();
    addElements:
        for (const quad of resource) {
            const data = quad.subject.termType == "BlankNode" ? blank : named;
            if (data.has(quad.subject.id)) {
                const props = data.get(quad.subject.id)!;
                if (props.has(quad.predicate.id)) {
                    // check if value is already in array, if it is, dont add it anymore
                    const objs = props.get(quad.predicate.id)!;
                    for (const obj of objs) {
                        // while it might offer better performance to use a set instead
                        // of an array, the custom type Quad_Object would not work correctly
                        // with Set.has(), and thus would require a seperate container storing
                        // the IDs (which would in turn not be memory efficient)
                        if (obj.equals(quad.object)) {
                            continue addElements;
                        }
                    }
                    objs.push(quad.object);
                } else {
                    props.set(quad.predicate.id, new Array(quad.object));
                }
            } else {
                data.set(quad.subject.id, new Map([[quad.predicate.id, new Array(quad.object)]]));
            }
        }
    // converting all the entries of the blank map first
    // with the ordered view done, a more compact turtle string can be generated
    const writer = new Writer({prefixes: _prefixes});
    for (const [subject, properties] of named) {
        for (const [predicate, objects] of properties) {
            for (const object of objects) {
                if (object.termType != "BlankNode") {
                    writer.addQuad(namedNode(subject), namedNode(predicate), object);
                } else {
                    const blankProperties = blank.get(object.id)!;
                    for (const [blankPredicate, blankObjects] of blankProperties) {
                        for (const blankObject of blankObjects) {
                            writer.addQuad(
                                namedNode(subject), namedNode(predicate),
                                writer.blank(namedNode(blankPredicate), blankObject)
                            );
                        }
                    }
                }
            }
        }
    }
    let str: string = "";
    writer.end((_, result) => str = result);
    return str;
}

/**
 * Adds all the resources from each bucket entry of the BucketResources object to the specified container
 * Note: currently does not do any error handling
 *  handling should be something in the line of collecting all the resources that were added OR trying to add them again?
 *
 * @param bucketResources
 * @param metadata
 * @param ldpComm
 * @returns {Promise<void>}
 */
export async function addResourcesToBuckets(bucketResources: BucketResources, metadata: LDESMetadata, ldpComm: LDPCommunication, prefixes: any): Promise<void> {
    for (const containerURL of Object.keys(bucketResources)) {
        for (const resource of bucketResources[containerURL]) {
            const response = await ldpComm.post(containerURL, resourceToOptimisedTurtle(resource, prefixes))
            // console.log(`Resource stored at: ${response.headers.get('location')} | status: ${response.status}`)
            // TODO: handle when status is not 201 (Http Created)
        }
    }
}

/**
 * Checks if the stream at `eventStreamURL` with name `eventStreamID` has a shape set
 * 
 * @param eventStreamURL
 * @param eventStreamID
 * @param comm
 * @returns {Promise<boolean>}
 */
export async function streamHasShape(
    eventStreamURL: string,
    eventStreamID: string,
    comm: Communication = new LDPCommunication()
): Promise<boolean> {
    // looking for all triples with subject eventStreamURI located at eventStreamURL
    const store = await turtleStringToStore(
        await (await comm.get(eventStreamURL + ".meta")).text()
    )
    // check if there are quads having a tree.shape member
    return store.getQuads(`${eventStreamURL}#${eventStreamID}`, TREE.shape, null, null).length != 0;
}

/**
 * Similar to `addShapeToLil`, but creates a shape based on the resource
 * 
 * @param eventStreamURI The entire event stream URI (e.g. http://localhost:3000/stream#EventStream)
 * @param resource A single resource, used to generate a shape with
 * @param resourcePrefixes (optional) Prefixes used to represent the resource in a bucket (for a more compact turtle string)
 * @param comm (optional) Communication that should be used (e.g. existing Solid session)
 * @param shapeLocation (optional) Name of the shape resource
 * @returns {Promise<boolean>} `true` on success, `false` when a shape is already present.
 * Throws Error() when requests are unsuccessful
 */
export async function addShapeToLilFromResource(
    eventStreamURI: string,
    resource: Resource,
    resourcePrefixes: any = {},
    comm: Communication = new LDPCommunication(),
    shapeLocation: string = "shape.shacl"
) : Promise<boolean> {
    const [eventStreamURL, /* Unused eventStreamID */] = eventStreamURI.split("#");
    const shape = generateShape(resource, eventStreamURL);
    return await addShapeToLil(
        eventStreamURI, shape, resourcePrefixes, comm, shapeLocation
    );
}

/**
 * Adds a shape to the given event stream, by publishing the shape.shacl file with given prefixes
 * and adding the tree:shape triple to the LDES metadata
 * 
 * @param eventStreamURI The entire event stream URI (e.g. http://localhost:3000/stream#EventStream)
 * @param shape The shape that should be published (not validated here)
 * @param resourcePrefixes (optional) Prefixes used to represent the resource in a bucket (for a more compact turtle string)
 * @param comm (optional) Communication that should be used (e.g. existing Solid session)
 * @param shapeLocation (optional) Name of the shape resource
 * @returns {Promise<boolean>} `true` on success, `false` when a shape is already present.
 * Throws Error() when requests are unsuccessful
 */
 export async function addShapeToLil(
    eventStreamURI: string,
    shape: Resource,
    resourcePrefixes: any = {},
    comm: Communication = new LDPCommunication(),
    shapeLocation = "shape.shacl"
): Promise<boolean> {
    const [eventStreamURL, eventStreamID] = eventStreamURI.split("#");
    // 0: check if shape is present first
    if (await streamHasShape(eventStreamURL, eventStreamID, comm)) {
        return false;
    }
    // 1: publish the shape resource using resourceToOptimisedTurtle &
    // ldpComm.post to a new shape, as well as using prefixes with shape support
    let response = await comm.put(
        eventStreamURL + shapeLocation,
        resourceToOptimisedTurtle(
            shape, {...resourcePrefixes, "sh": "http://www.w3.org/ns/shacl#", "": eventStreamURL}
        )
    );
    if (response.status < 200 || 299 < response.status) {
        throw Error(`Error occurred while adding shape content; got status ${response.status} - ${response.statusText}`);
    }
    // 2: update ldes metadata to add tree:shape
    const updateStore = new Store([new Quad(
        namedNode(eventStreamURI),
        namedNode(TREE.shape),
        namedNode(eventStreamURL + shapeLocation)
    )]);
    response = await comm.patch(eventStreamURL + ".meta", `INSERT DATA {${storeToString(updateStore)}}`);
    if (response.status < 200 || 299 < response.status) {
        throw Error(`Error occurred while setting shape property to LDES; got status ${response.status} - ${response.statusText}`);
    }
    return true;
}