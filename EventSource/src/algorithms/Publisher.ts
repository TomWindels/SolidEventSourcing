import { literal, namedNode } from "@rdfjs/data-model";
import { 
    BucketizeStrategy,
    Communication,
    LDES,
    LDESinLDP,
    LDESinLDPClient,
    LDP,
    MetadataParser,
    patchSparqlUpdateInsert,
    TREE,
    turtleStringToStore,
    ViewDescription
} from "@treecg/versionawareldesinldp";
import { Logger } from "@treecg/versionawareldesinldp/dist/logging/Logger";
import { Quad, Store } from "n3";
import { Resource } from "../util/EventSource";

/**
 * Base Publisher class for timeseries data publishing, with buckets using a date representation
 * Different implementations use different strategies, having their own benefits and drawbacks
 */
export abstract class Publisher {

    // methods that should be available to publisher users (= interface)

    // adds a resource to the queue to be published/publishes directly
    // (implementation dependant)
    public abstract publish(resource: Resource | Resource[]): Promise<void>;
    // rebalances buckets if this is valuable (but not necessary, as these
    // happen automatically)
    // e.g. spreading resources out accross buckets, reducing the variance
    // in quantity per bucket, or reducing the number of buckets when
    // too fragmented (this is implementation dependant)
    public abstract rebalance(): Promise<void>;
    // guarantees that the queue of resources is published and then flushed
    // uppon return when no error is thrown
    public abstract flush(): Promise<void>;

    // internal methods that should be implemented by publisher implementations

    // implementation specific resource(s) to string converter
    protected abstract generateString(resource: Resource, prefixes: any): string;

    // properties only available for reading by publisher implementations
    private _treePath: string = ""; // when not set, infinity is used
    protected get treePath(): string { return this._treePath; };
    private _bucketSize: number = Infinity; // when not set, infinity is used
    protected get bucketSize(): number { return this._bucketSize; };
    private _samplesPerResource: number = Infinity; // when not set, infinity is used
    protected get samplesPerResource(): number { return this._samplesPerResource; };
    private _buckets: Date[] = [];
    protected get buckets(): Date[] { return this._buckets; };
    protected readonly logger: Logger;

    public get uri() : string {
        return this.lil.LDESinLDPIdentifier;
    }
    public get url() : string {
        return this.lil.LDESinLDPIdentifier.substring(0, this.lil.LDESinLDPIdentifier.indexOf("#"));
    }

    private comm: Communication;
    private lil: LDESinLDP;
    private prefixes: any;

    constructor(
        comm: Communication,
        url: string,
        logger?: Logger
    ) {
        this.comm = comm;
        this.logger = logger ?? new Logger(this);
        // cannot use `this.url` yet, as it depends on `this.lil`
        // checking wether or not the URL is valid (and not ending on #EventStream)
        try {
            const test = new URL(url)
            if (url.endsWith("#EventStream")) {
                url = url.substring(0, url.length - "#EventStream".length);
            }
            this.lil = new LDESinLDP(url, this.comm);
        } catch (e) {
            logger?.error(`Invalid URL used for the creation of a LIL (Publisher): ${url}`);
            // throwing the error back as the Publisher is invalid
            throw e;
        }
    }

    // internal methods used by publisher implementations

    /**
     * Has to be called in an async initialise method.
     * As long as this hasn't completed yet, the LIL is possibly invalid
     * 
     * @param config The config for the LIL, only used when there was no LIL present
     *  before
     */
    public async init(
        treePath: string,
        initialDate: Date = new Date(),
        options: {
            prefixes?: any,
            shape?: {
                content: Resource,
                path: string
            },
            bucketSize: number,
            samplesPerResource: number
        } = {
            bucketSize: Infinity,
            samplesPerResource: 1
        }
    ): Promise<boolean> {
        this.prefixes = options.prefixes;
        this._treePath = treePath;
        let status = await this.lil.status();
        let valid: boolean;
        if (!status.found) {
            // attempting to create an ldes using the provided config
            // NOTE: shape is not added here, this is checked for later in a general case
            // for both pre-existing and new LDES's
            await this.lil.initialise({
                treePath: treePath, pageSize: options.bucketSize, date: initialDate
            });
            // check the status again and re-evaluate
            status = await this.lil.status();
            valid = status.valid
        } else if (!status.valid) {
            // cannot be used, logging
            this.logger.warn(`Invalid container found at "${this.url}". Publisher cannot be used.`)
            valid = false
        } else {
            valid = status.valid
        }
        if (!valid) {
            // nothing more can be done to this location at this time, so stopping here
            return false;
        }
        // expanding LDES properties (it is valid) by observing the current state of the LDES
        const metadata = MetadataParser.extractLDESinLDPMetadata(await this.lil.readMetadata())
        const newMetadata = new Store();
        // -- 1: getting the bucket size and samples per resource if set
        if (metadata.fragmentSize == Infinity && options.bucketSize != Infinity && status.empty) {
            // setting bucket size and updating metadata
            this._bucketSize = options.bucketSize;
            // updating using the existing ViewDescription, or by creating one
            if (metadata.view.viewDescription == undefined) {
                // new ViewDescription is required, so making one here and using the new store
                //  from metadata.view as patch data (so the relation to ViewDescription is included
                //  as well)
                metadata.view.viewDescription = new ViewDescription(
                    this.url + "viewDescription",
                    new LDESinLDPClient(
                        this.url + "client",
                        new BucketizeStrategy(
                            this.url + "bucketizeStrategy",
                            LDES.timestampFragmentation,
                            treePath,
                            this.bucketSize
                        )
                    ),
                    metadata.eventStreamIdentifier,
                    metadata.rootNodeIdentifier
                );
                newMetadata.addQuads(metadata.view.getStore().getQuads(null, null, null, null));
            } else {
                // view description already exists, so simply adding the bucket size to its properties
                newMetadata.addQuad(
                    namedNode(metadata.view.viewDescription.managedBy.bucketizeStrategy.id),
                    namedNode(LDES.pageSize),
                    literal(this._bucketSize.toString())
                );
            }
        }
        // FIXME: get from metadata if set
        this._samplesPerResource = options.samplesPerResource
        // adding the shape to the remote location if needed and required
        if (options.shape != undefined && metadata.shape == undefined && status.empty) {
            // publishing the shape to the remote location
            const shapeURL = this.url + options.shape.path;
            let response = await this.comm.put(
                shapeURL,
                this.generateString(
                    options.shape.content, {...this.prefixes, "sh": "http://www.w3.org/ns/shacl#", "": this.url}
                )
            );
            if (response.status < 200 || 299 < response.status) {
                this.logger.error(`Error occurred while adding shape content; got status ${response.status} - ${response.statusText}`);
                return false;
            }
            // adding the property to the LDES as well
            newMetadata.addQuad(namedNode(metadata.eventStreamIdentifier), namedNode(TREE.shape), namedNode(shapeURL));
        }
        // syncing buckets with remote
        // TODO this.buckets. ...
        // applying all the changes
        const containerIdentifier = this.lil.LDESinLDPIdentifier;
        const insertBody = patchSparqlUpdateInsert(newMetadata);
        // FIXME check if this is correct: containerIdentifier for patch instead of URL?
        await this.comm.patch(containerIdentifier, insertBody);
        // enabling the internal data manipulation methods
        this._addToBucket = (content: string, bucketUrl: string) => {
            // manual patching required, as the content is already formed by a
            //  custom string method
            this.comm.patch(bucketUrl, `INSERT DATA {${content}}`)
        }
        this._removeFromBucket = (subject: string, bucketUrl: string) => {
            // TODO
        }
        this._createBucket = (date: Date) => {
            // chosen date for bucket name depends on the implementation
            this.lil.newFragment(date)
            // add it to the list of known buckets
            this._buckets.push(date)
        }
        // only now can the publisher functions be used
        return true;
    }

    // adds the resource (already in string representation) to a bucket
    protected async append(resource: Resource, bucket: Date) {
        const bucketUrl = this.url + bucket.getTime();
        if (!this._buckets.includes(bucket)) {
            // creating a new bucket
            this._createBucket(bucket);
            // adding the bucket to the list of known buckets
            this._buckets.push(bucket);
            // resorting buckets
            this._buckets.sort();
        }
        const additionalBucketContent = this.generateString(resource, this.prefixes);
        // adding resource to bucket using internal method
        this._addToBucket(additionalBucketContent, bucketUrl);
    }

    // moves the resource (by subject) from the original bucket to the new one
    protected async move(resource: string, from: Date, to: Date) {
        // as a check, see that the original bucket still has the resource
        // TODO: in a locked context, this check wouldnt be required
        if ((await this.getMembers(from)).indexOf(resource) == -1) {
            this.logger.error(`Tried to move resource "${resource}" from bucket "${from.getTime()}", but it was not found.`);
            return;
        }
        // get the resource data from the original bucket
        // TODO
        const data: Resource = []
        // remove from original bucket
        this._removeFromBucket(resource, this.url + from.getTime());
        // add the original data to the new bucket
        this.append(data, to);
    }

    protected async getMembers(bucket: Date): Promise<string[]> {
        const bucketUrl = this.url + bucket.getTime();
        const containerResponse = await this.comm.get(bucketUrl);
        return (await turtleStringToStore(await containerResponse.text(), bucketUrl))
            .getQuads(bucketUrl, LDP.contains, null, null)
            .map((quad: Quad) => { return quad.subject.value });
    }

    // private helper methods only protected methods here can use
    // initially non functioning, can only be enabled after calling init(), and
    // containing a valid LIL structure
    private _addToBucket = (content: string, bucketUrl: string): void => {
        throw Error("Publisher was either not initialised, or is no longer valid");
    }

    private _removeFromBucket = (subject: string, bucketUrl: string): void => {
        throw Error("Publisher was either not initialised, or is no longer valid");
    }

    private _createBucket = (bucketName: Date): void => {
        throw Error("Publisher was either not initialised, or is no longer valid");
    }

}