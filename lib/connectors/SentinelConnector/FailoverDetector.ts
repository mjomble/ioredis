import { Debug } from "../../utils";
import SentinelConnector from "./index";

const debug = Debug("FailoverDetector");

const CHANNEL_NAME = "+switch-master";

export class FailoverDetector {
  private connector: SentinelConnector;
  private sentinel;

  // client can't be used for regular commands after this
  constructor(connector: SentinelConnector, client) {
    this.connector = connector;
    this.sentinel = client;

    this.subscribe();
  }

  public disconnect() {
    this.sentinel.disconnect();
  }

  private subscribe() {
    debug("Starting FailoverDetector on sentinel");
    this.sentinel.subscribe(CHANNEL_NAME);

    // TODO detect lost/hanging connection to sentinel

    this.sentinel.on("message", (channel: string) => {
      if (channel === CHANNEL_NAME) {
        debug("Failover detected, disconnecting");

        // TODO start with regular disconnect, fallback to forced after timeout?
        this.connector.forceDisconnect();
      }
    });
  }
}
