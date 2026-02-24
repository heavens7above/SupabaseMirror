const logger = require('./logger');

class WebhookRegistry {
  constructor() {
    this.handlers = new Map();
  }

  /**
   * Registers a single handler for an event type.
   * Enforces the single-subscriber rule.
   */
  register(eventType, handler) {
    if (this.handlers.has(eventType)) {
      const errorMsg = `Conflict - Multiple listeners registered for event: ${eventType}`;
      logger.error(`[CRITICAL ALERT] ${errorMsg}`, { eventType, alertLevel: 'red' });
      throw new Error(errorMsg);
    }
    this.handlers.set(eventType, handler);
    logger.info(`Registered handler for webhook event: ${eventType}`);
  }

  /**
   * Returns an Express middleware/handler mapped to this event type.
   */
  dispatch(eventType) {
    return async (req, res, next) => {
      const handler = this.handlers.get(eventType);
      if (!handler) {
        logger.warn(`No handler registered for event: ${eventType}`);
        return res.status(404).send("Handler not found");
      }
      try {
        await handler(req, res);
      } catch (err) {
        logger.error(`Error in handler for event ${eventType}: ${err.message}`, { error: err.stack });
        if (!res.headersSent) {
          res.status(500).send("Internal Server Error");
        }
      }
    };
  }
  
  // For testing
  _clear() {
    this.handlers.clear();
  }
}

const registry = new WebhookRegistry();

module.exports = {
  registry,
  WebhookRegistry // export class for testing independent scope
};
