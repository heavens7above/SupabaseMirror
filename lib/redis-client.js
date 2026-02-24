const Redis = require('ioredis');
require('dotenv').config();

const redisUrl = process.env.REDIS_URL;

if (!redisUrl) {
  throw new Error('Missing REDIS_URL environment variable. Please add it to your Railway Variables tab.');
}

const redis = new Redis(redisUrl, {
  connectTimeout: 5000,
  maxRetriesPerRequest: 1,
});

redis.on('error', (err) => {
  // Silent error logging to avoid crashing during non-critical tasks like backfill
  // console.warn('Redis connection issue:', err.message);
});

module.exports = redis;
