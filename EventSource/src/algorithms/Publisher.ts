import { Communication, extractLdesMetadata, LDESinLDP, LDESMetadata } from "@treecg/versionawareldesinldp";
import { Resource } from "../util/EventSource";

abstract class Publisher {

    // methods that should be available to publisher users (= interface)

    // adds a resource to the queue to be published/publishes directly
    // (implementation dependant)
    public abstract publish(resource: Resource): Promise<void>;
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
    // bucket url)
    protected abstract assignResourceToBucket(resource: Resource): string;

    protected abstract resourceToString(resource: Resource): string;
    // TODO: others

    protected metadata: LDESMetadata | undefined;

    private eventStreamURL: string;
    private eventStreamID: string;
    public get uri() : string {
        return `${this.eventStreamURL}#${this.eventStreamID}`;
    }
    private comm: Communication;
    private lil: LDESinLDP;

    constructor(
        comm: Communication,
        uri: string
    ) {
        this.comm = comm;
        [this.eventStreamURL, this.eventStreamID] = uri.split("#");
        this.lil = new LDESinLDP(this.eventStreamURL, comm);
        this.syncMetadata().then(() => {
            // initialise LDES if the metadata is
            // undefined (LDES does not yet exist)
            // TODO: maybe use exists() on lil instead?
            if (!this.metadata) {
                // TODO use extra optional arguments in ctor to create LDES
            }
        });
        /* Other stuff */
    }

    // internal methods used by publisher implementations

    // adds the resource (already in string representation) to a bucket,
    // with the bucket being selected by an implementation specific approach
    protected async addResourceToBucket(resource: Resource) {
        // using the bucket created by the implementation
        const bucketUrl = this.assignResourceToBucket(resource);
        await this.syncMetadata();
        // TODO: check in metadata if bucket url exists
        // create bucket if required
        const additionalBucketContent = this.resourceToString(resource);
        // adding resource to bucket using internal method
        this._addToBucket(additionalBucketContent, bucketUrl);
    }

    protected async syncMetadata(): Promise<void> {
        const store = await this.lil.readMetadata();
        this.metadata = extractLdesMetadata(store, this.uri);
    }

    // private helper methods only protected methods here can use
    private _addToBucket(content: string, bucketUrl: string) {
        this.comm.patch(bucketUrl, `INSERT DATA {${content}}`)
    }

    private _createBucket(bucketUrl: string) {
        /* */
    }

}