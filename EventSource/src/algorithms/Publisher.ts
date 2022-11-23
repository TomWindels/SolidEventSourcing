import { literal, namedNode } from "@rdfjs/data-model";
import { Communication, LDES, LDESinLDP, MetadataParser, TREE, ViewDescription } from "@treecg/versionawareldesinldp";
import { Logger } from "@treecg/versionawareldesinldp/dist/logging/Logger";
import { Store } from "n3";
import { Resource } from "../util/EventSource";

abstract class Publisher {

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

    // assigns a resource to a bucket url (can be either an existing or new
    // bucket url) by providing a date (which is encoded in the bucket url)
    // when `resource` is undefined, a first general bucket name is expected
    // e.g. bucketizing per year would have return a 01/01/yyyy - 00:00z date
    // on resource == undefined
    protected abstract getBucket(resource?: Resource): Date;

    protected abstract generateString(resource: Resource, prefixes: any): string;
    // TODO: others

    // properties only available for reading by publisher implementations
    private _bucketSize: number = Infinity; // when not set, infinity is used
    protected get bucketSize(): number { return this._bucketSize; };
    private _samplesPerResource: number = Infinity; // when not set, infinity is used
    protected get samplesPerResource(): number { return this._samplesPerResource; };
    private _buckets: Date[] = [];
    protected get buckets(): Date[] { return this._buckets; };

    public get uri() : string {
        return this.lil.LDESinLDPIdentifier;
    }
    public get url() : string {
        return this.lil.LDESinLDPIdentifier.substring(0, this.lil.LDESinLDPIdentifier.indexOf("#"));
    }

    private logger: Logger;
    private comm: Communication;
    private lil: LDESinLDP;
    private prefixes: any;

    constructor(
        comm: Communication,
        uri: string,
        logger?: Logger
    ) {
        this.comm = comm;
        this.logger = logger ?? new Logger(this);
        // cannot use `this.url` yet, as it depends on `this.lil`
        this.lil = new LDESinLDP(uri.substring(0, uri.indexOf("#")), this.comm);
    }

    // internal methods used by publisher implementations

    /**
     * Has to be called in an async initialise method.
     * As long as this hasn't completed yet, the LIL is possibly invalid
     * 
     * @param config The config for the LIL, only used when there was no LIL present
     *  before
     */
    protected async init(
        treePath: string,
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
        let status = await this.lil.status();
        let valid: boolean;
        if (!status.found) {
            // attempting to create an ldes using the provided config
            // NOTE: shape is not added here, this is checked for later in a general case
            // for both pre-existing and new LDES's
            await this.lil.initialise({
                treePath: treePath, pageSize: options.bucketSize, date: this.getBucket()
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
                metadata.view.viewDescription = new ViewDescription(/*TODO*/);
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
        // applying all the changes
        const containerIdentifier = this.lil.LDESinLDPIdentifier;
        const insertBody = patchInsert(newMetadata);
        // FIXME check if this is correct: containerIdentifier for patch instead of URL?
        await this.comm.patch(containerIdentifier, insertBody);
        // enabling the internal data manipulation methods
        this._addToBucket = (content: string, bucketUrl: string) => {
            // TODO: use better general patch method for query generation
            this.comm.patch(bucketUrl, `INSERT DATA {${content}}`)
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

    // adds the resource (already in string representation) to a bucket,
    // with the bucket being selected by an implementation specific approach
    protected async append(resource: Resource) {
        // using the bucket created by the implementation
        const bucket = this.getBucket(resource);
        const bucketUrl = this.url + bucket;
        if (!this._buckets.includes(bucket)) {
            // creating a new bucket
            this._createBucket(bucket);
            // adding the bucket to the list of known buckets
            this._buckets.push(bucket);
        }
        // TODO: check in metadata if bucket url exists
        // create bucket if required
        const additionalBucketContent = this.generateString(resource, this.prefixes);
        // adding resource to bucket using internal method
        this._addToBucket(additionalBucketContent, bucketUrl);
    }

    // private helper methods only protected methods here can use
    // initially non functioning, can only be enabled after calling init(), and
    // containing a valid LIL structure
    private _addToBucket = (content: string, bucketUrl: string): void => {
        throw Error("Publisher was either not initialised, or is no longer valid");
    }

    private _createBucket = (bucketName: Date): void => {
        throw Error("Publisher was either not initialised, or is no longer valid");
    }

}