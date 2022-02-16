const highwayhash = require("highwayhash-nodejs");

const getInstance = (config) => {
  if (config.highwayHashKey !== undefined) {
    highwayhash.resetKey(Buffer.from(config.highwayHashKey, "hex"));
  }
  const redisClient = config.redisClient;

  const publishInvalidationMessage = async (key, field) => {
    const channel = `{${key}}:topic`;

    const type = Buffer.alloc(1);
    type[0] = 0x01;

    const excludedId = Buffer.alloc(16).fill(0x00);
    const hashesCount = Buffer.from([0x00, 0x00, 0x00, 0x01]);
    const fieldHashBuffer = highwayhash.hash128(Buffer.from(field));

    const msg = Buffer.alloc(
      type.length +
        excludedId.length +
        hashesCount.length +
        fieldHashBuffer.length
    );
    type.copy(msg, 0);
    excludedId.copy(msg, 1);
    hashesCount.copy(msg, 17);
    fieldHashBuffer.copy(msg, 21);

    await redisClient.publish(channel, msg);

    // const res = await redisClient.eval(
    //   `local v = redis.call("hget", KEYS[1], ARGV[1]); redis.call("hset", KEYS[1], ARGV[1], ARGV[2]); redis.call("publish", KEYS[2], ARGV[3]); return v;`,
    //   2,
    //   key,
    //   channel,
    //   field,
    //   value,
    //   msg.toString('hex')
    // );
  };

  return {
    putAsync: async (key, field, value, callback) => {
      try {
        await redisClient.hSet(key, field, value);
        await publishInvalidationMessage(key, field);
      } catch (err) {
        if (callback) callback(undefined, err);
      }
    },
    removeAsync: async (key, field, callback) => {
      try {
        await redisClient.hDel(key, field);
        await publishInvalidationMessage(key, field);
      } catch (err) {
        if (callback) callback(undefined, err);
      }
    },
  };
};

module.exports = { getInstance };
