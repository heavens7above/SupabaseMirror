const Redis = require('ioredis');
require('dotenv').config();

const redisUrl = process.env.REDIS_URL;

if (!redisUrl) {
  throw new Error('Missing REDIS_URL');
}

const redis = new Redis(redisUrl);

module.exports = redis;
