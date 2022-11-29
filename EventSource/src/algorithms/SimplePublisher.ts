import { Communication, turtleStringToStore } from "@treecg/versionawareldesinldp";
import { Store } from "n3";
import { getTimeStamp, Resource, resourceToOptimisedTurtle } from "../util/EventSource";
import { Publisher } from "./Publisher";

/**
 * Example simple publisher implementation, without any buffering
 */
class SimplePublisher extends Publisher {

    bucketData: [Date, string[]][] = new Array();

    constructor(communication: Communication, ldesURL: string) {
        super(communication, ldesURL);
    }

    public async publish(resource: Resource | Resource[]): Promise<void> {
        // no resource grouping/buffering/... happening here, nor overflow checks
        // of existing buckets, so directly appending
        // it does flatten the resource array where needed, so grouped resources
        // should be correctly grouped beforehand
        const flattened = resource.flat()
        await this.append(flattened, this.getBucket(flattened));
    }

    public async rebalance(): Promise<void> {
        // a simple rebalance, checking if none of the buckets are overflowing, and
        // rebalancing those that are
        // TODO: if the LDES is locked, this could remain cached
        this.bucketData.length = 0;
        for (const bucket of this.buckets) {
            this.bucketData.push([bucket, await this.getMembers(bucket)]);
        }
        // with the updated bucket data, rebalancing can be done
        for (const [i, [bucket, members]] of this.bucketData.entries()) {
            let remaining = members.length - this.bucketSize;
            // remaining > 0: rebalancing required
            if (remaining > 0) {
                this.logger.info(`Rebalancing bucket "${bucket.getTime()}" - ${remaining} items remaining.`);
                // applying rebalancing
                if (i > 0) {
                    remaining -= await this._rebalance(this.bucketData[i], this.bucketData[i - 1]);
                }
                if (i < this.buckets.length - 1) {
                    remaining -= await this._rebalance(this.bucketData[i], this.bucketData[i + 1]);
                }
                while (remaining > 0) {
                    // adding a new buckets, based on the currently oldest sample, as there is still
                    // rebalancing required
                    const newBucket = new Date(/* TODO */);
                    this.logger.info(`Rebalancing requires a new bucket, so creating bucket "${newBucket.getTime()}".`);
                    // initialising metadata for this bucket, as _rebalance uses this data
                    const metadata: [Date, string[]] = [newBucket, []]
                    this.bucketData.push(metadata);
                    // moving over
                    remaining -= await this._rebalance(this.bucketData[i], metadata);
                }
            }
        }
    }

    public async flush(): Promise<void> {
        // bufferless implementation, so nothing has to be done here
    }

    // This method can throw if the resource is defined but invalid (missing/incorrect
    //  treePath property) or too old (older than oldestSampleDate from the ctor).
    // This implementation does not create new buckets here, this happens in `rebalance` only,
    //  meaning that the best possible bucket of the ones already existing is chosen.
    private getBucket(resource: Resource): Date {
        // Extracting the date of the resource, and using the date closest
        //  to this one but in the past, if any
        try {
            const resourceDate = new Date(getTimeStamp(resource, this.treePath));
            // When this method is called, buckets are considered to be sorted. Using the `findIndex` method,
            //  the best fitting bucket is selected, based on the index of the first bucket exceeding the
            //  timestamp of the sample.
            // This approach can definitely still be improved upon, but it is sufficient for a simple
            //  proof of concept approach.
            const bucketIndex = this.buckets.findIndex((bucketDate: Date) => { return bucketDate > resourceDate; });
            if (bucketIndex == 0) {
                // all existing buckets are too new, this is not supported by this simple proof of concept
                throw Error("Invalid sample received: samples older than the oldest (initial) bucket are not supported.");
            } else if (bucketIndex == -1) {
                // all of the existing buckets are older than the sample, so the last bucket (newest one) is the best one
                return this.buckets[this.buckets.length - 1];
            } else {
                // found a bucket in between as best match, using that one
                return this.buckets[bucketIndex - 1];
            }    
        } catch (e) {
            throw Error(`Invalid sample received: sample does not contain a valid tree:path (${this.treePath}) member representing a usable date.`)
        }
    }

    // tries to move `amount` resources from bucket `from` to bucket `to`, returning
    // the actual amount moved (e.g. 0 when the to-bucket has no space left)
    private async _rebalance(from: [Date, string[]], to: [Date, string[]]): Promise<number> {
        // checking the amount that can be moved
        const amount = Math.min(from[1].length - this.bucketSize, to[1].length - this.bucketSize);
        for (let i = 0; i < amount; ++i) {
            // TODO: better resource selection
            const resource = from[1].pop()!!;
            // moving in remote
            await this.move(resource, from[0], to[0]);
            // updating metadata
            to[1].push(resource);
        }
        return amount;
    }

    protected generateString(resource: Resource, prefixes: any): string {
        return resourceToOptimisedTurtle(resource, prefixes);
    }

}