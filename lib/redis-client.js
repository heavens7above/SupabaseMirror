require('dotenv').config();
const Redis = require('ioredis');
const logger = require('./logger');

const redisUrl = process.env.REDIS_URL;

if (!redisUrl) {
  throw new Error('Missing REDIS_URL environment variable. Please add it to your Railway Variables tab.');
}

const redis = new Redis(redisUrl, {
  connectTimeout: 10000,
  maxRetriesPerRequest: null, // CRITICAL: Queue commands when connection is down
  enableOfflineQueue: true,   // Ensure burst commands are queued not dropped
  commandTimeout: 15000,      // Increase to 15s for heavy stress stability
  retryStrategy(times) {
    const delay = Math.min(times * 100, 3000);
    return delay;
  },
});

redis.on('connect', () => logger.info('Redis connected'));
redis.on('disconnect', () => logger.warn('Redis disconnected'));
redis.on('error', (err) => {
  logger.error('Redis connection error:', err.message);
});

module.exports = redis;
