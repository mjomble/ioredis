import { Debug } from "../../utils";
import SentinelConnector, { IRedisClient, ISentinelAddress } from "./index";

export interface ISentinel {
  address: Partial<ISentinelAddress>;
  client: IRedisClient;
}

const debug = Debug("FailoverDetector");

const CHANNEL_NAME = "+switch-master";

export class FailoverDetector {
  private connector: SentinelConnector;
  private sentinels: ISentinel[];
  private isDisconnected = false;

  // sentinels can't be used for regular commands after this
  constructor(connector: SentinelConnector, sentinels: ISentinel[]) {
    this.connector = connector;
    this.sentinels = sentinels;
  }

  public cleanup() {
    for (const sentinel of this.sentinels) {
      sentinel.client.disconnect();
    }
  }

  public subscribe() {
    debug("Starting FailoverDetector on sentinels");

    const promises: Promise<unknown>[] = [];

    for (const sentinel of this.sentinels) {
      const promise = sentinel.client.subscribe(CHANNEL_NAME).catch((err) => {
        debug(
          "Failed to subscribe to failover messages on sentinel %s:%s (%s)",
          sentinel.address.host || "127.0.0.1",
          sentinel.address.port || 26739,
          err.message
        );
      });

      promises.push(promise);

      // TODO detect lost/hanging connections

      sentinel.client.on("message", (channel: string) => {
        if (!this.isDisconnected && channel === CHANNEL_NAME) {
          this.disconnect();
        }
      });
    }

    return Promise.all(promises);
  }

  private disconnect() {
    // Avoid disconnecting more than once per failover.
    // A new FailoverDetector will be created after reconnecting.
    this.isDisconnected = true;

    debug("Failover detected, disconnecting");

    // Will call this.cleanup()
    this.connector.disconnect();
  }
}
