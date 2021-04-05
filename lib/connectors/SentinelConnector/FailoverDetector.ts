import { Debug } from "../../utils";
import SentinelConnector, { IRedisClient } from "./index";

const debug = Debug("FailoverDetector");

const CHANNEL_NAME = "+switch-master";

export class FailoverDetector {
  private connector: SentinelConnector;
  private sentinels: IRedisClient[];
  private isDisconnected = false;

  // sentinels can't be used for regular commands after this
  constructor(connector: SentinelConnector, sentinels: IRedisClient[]) {
    this.connector = connector;
    this.sentinels = sentinels;
  }

  public subscribe() {
    debug("Starting FailoverDetector on sentinels");

    const promises: Promise<unknown>[] = [];

    for (const sentinel of this.sentinels) {
      const promise = sentinel.subscribe(CHANNEL_NAME).catch((err) => {
        console.error("Failed to subscribe to Redis failover events:", err); // TODO ErrorEmitter
      });

      promises.push(promise);

      // TODO detect lost/hanging connections

      sentinel.on("message", (channel: string) => {
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

    for (const sentinel of this.sentinels) {
      sentinel.disconnect();
    }

    this.connector.disconnect();
  }
}
