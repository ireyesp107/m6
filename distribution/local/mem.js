const mem = {};

const memMap = new Map();

mem.get = function(key, cb) {
  // to return the full list of keys available to the store if no key is passed
  cb = cb || function() { };
  let actualKey;
  let actualGid = 'default';
  if (typeof key == 'string' || key == null) {
    actualKey = key;
  } else if (typeof key == 'object' && ('key' in key) && ('gid' in key)) {
    actualKey = key.key;
    actualGid = key.gid;
  } else {
    cb(new Error('not valid key'));
    return;
  }

  if (actualKey == null) {
    if (memMap.has(actualGid)) {
      cb(null, [...memMap.get(actualGid).keys()]);
    } else {
      cb(null, []);
    }
    return;
  }

  if (memMap.has(actualGid)) {
    const validKeys = memMap.get(actualGid);
    if (validKeys.has(actualKey)) {
      cb(null, validKeys.get(actualKey));
    } else {
      cb(new Error('key not found in group' +
      actualGid + ' ' + actualKey + ', ' +
       JSON.stringify(global.nodeConfig) + '_____' +
      JSON.stringify([...memMap.get(actualGid)])));
    }
  } else {
    cb(new Error('key not found'));
  }
};
mem.put = function(obj, key, cb) {
  // If no primary key is provided to put,
  // by passing the value null instead of a key,
  // then the system uses the sha256 hash of the
  // serialized object about to be stored as key.

  cb = cb || function() { };
  let actualKey;
  let actualGid = 'default';
  if (typeof key == 'string' || key == null) {
    actualKey = key;
  } else if (typeof key == 'object' && ('key' in key) && ('gid' in key)) {
    actualKey = key.key;
    actualGid = key.gid;
  } else {
    cb(new Error('not valid key'));
    return;
  }

  if (actualKey == null) {
    actualKey = global.distribution.util.id.getID(obj);
  }

  if (typeof obj != 'object') {
    cb(new Error('invalid object'));
    return;
  }

  if (!memMap.has(actualGid)) {
    memMap.set(actualGid, new Map());
  }

  memMap.get(actualGid).set(actualKey, obj);
  cb(null, obj);
};

mem.del = function(key, cb) {
  cb = cb || function() { };
  if (key == null) {
    cb(new Error('key is null'));
    return;
  }
  let actualKey;
  let actualGid = 'default';
  if (typeof key == 'string') {
    actualKey = key;
  } else if (typeof key == 'object' && ('key' in key) && ('gid' in key)) {
    actualKey = key.key;
    actualGid = key.gid;
  } else {
    cb(new Error('not valid key'));
    return;
  }

  if (memMap.has(actualGid)) {
    const validKeys = memMap.get(actualGid);
    // get valid gids of keys
    if (validKeys.has(actualKey)) {
      const obj = validKeys.get(actualKey);
      validKeys.delete(actualKey);
      cb(null, obj);
    } else {
      cb(new Error('key not found in group'));
    }
  } else {
    cb(new Error('key not found'));
    return;
  }
};

mem.append = function(obj, key, cb) {
  cb = cb || function() { };
  let actualKey;
  let actualGid = 'default';
  if (typeof key == 'string' || key == null) {
    actualKey = key;
  } else if (typeof key == 'object' && ('key' in key) && ('gid' in key)) {
    actualKey = key.key;
    actualGid = key.gid;
  } else {
    cb(new Error('not valid key'));
    return;
  }

  if (actualKey == null) {
    actualKey = global.distribution.util.id.getID(obj);
  }

  if (typeof obj != 'object') {
    cb(new Error('invalid object'));
    return;
  }

  if (!memMap.has(actualGid)) {
    memMap.set(actualGid, new Map());
  }

  if (memMap.get(actualGid).get(actualKey)) {
    obj = [...memMap.get(actualGid).get(actualKey), ...obj];
  }

  memMap.get(actualGid).set(actualKey, obj);
  cb(null, obj);
};

module.exports = mem;
