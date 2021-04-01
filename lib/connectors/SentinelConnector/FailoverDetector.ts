import { Debug } from "../../utils";
import SentinelConnector, { IRedisClient } from "./index";

const debug = Debug("FailoverDetector");

const CHANNEL_NAME = "+switch-master";

export class FailoverDetector {
  private connector: SentinelConnector;
  private sentinels: IRedisClient[];

  // sentinels can't be used for regular commands after this
  constructor(connector: SentinelConnector, sentinels: IRedisClient[]) {
    this.connector = connector;
    this.sentinels = sentinels;

    this.subscribe();
  }

  public disconnect() {
    for (const sentinel of this.sentinels) {
      sentinel.disconnect();
    }
  }

  private subscribe() {
    debug("Starting FailoverDetector on sentinels");

    let lastMasterIp = "";
    let lastMasterPort = "";

    for (const sentinel of this.sentinels) {
      sentinel.subscribe(CHANNEL_NAME).catch((err) => {
        console.error("Failed to subscribe to Redis failover events:", err); // TODO ErrorEmitter
      });

      // TODO detect lost/hanging connections

      sentinel.on("message", (channel: string, message: string) => {
        if (channel !== CHANNEL_NAME) {
          return;
        }

        // message example: "mymaster 172.26.0.4 6379 172.26.0.3 6379"
        const [, , , newIp, newPort] = message.split(" ");

        // We can get the same message about the same failover from multiple sentinels.
        // Make sure we only disconnect once per failover.
        if (newIp !== lastMasterIp || newPort !== lastMasterPort) {
          lastMasterIp = newIp;
          lastMasterPort = newPort;

          debug("Failover detected, disconnecting");
          this.connector.disconnect();
        }
      });
    }
  }
}
