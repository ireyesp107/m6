//  ________________________________________
// / NOTE: You should use absolute paths to \
// | make sure they are agnostic to where   |
// | your code is running from! Use the     |
// \ `path` module for that purpose.        /
//  ----------------------------------------
//         \   ^__^
//          \  (oo)\_______
//             (__)\       )\/\
//                 ||----w |
//                 ||     ||
// The service can use the key as the filename
// (in a pre-specified directory[1]), but it should
// protect against keys containing unsupported characters
// by converting keys to alphanumeric-only strings. It can
//  also use the serialization/deserialization library to
// write values to disk as strings and read strings from disk as values.
const fs = require('fs');
const path = require('path');
const dir = path.join(__dirname, '../../store' + '/' + global.nodeConfig.port);
// fs.mkdirSync(dir);
if (!fs.existsSync(dir)) {
  fs.mkdirSync(dir);
}

const store = {};

store.get = function(key, cb) {
  // to return the full list of keys available to the store if no key is passed
  // check if key given is object
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
  // concert keys to alpahanumeric-only strings

  if (!actualKey) {
    fs.readdir(path.join(dir, actualGid), (err, data) => {
      if (err) {
        cb(err);
      } else {
        cb(null, data);
      }
    });
  } else {
    actualKey = actualKey.replace(/[^a-zA-Z0-9]/g, '');

    // check if key as filename exists in my decided directory
    const fileName = path.join(dir, actualGid, actualKey);

    if (fs.existsSync(fileName)) {
      // if it does read file and deserealize value
      fs.readFile(fileName, {encoding: 'utf8'}, (err, data) => {
        if (err) {
          cb(err);
        } else {
          cb(null, global.distribution.util.deserialize(data));
        }
      });
    } else {
      cb(new Error('key does not exist'));
    }
  }
};
store.put = function(obj, key, cb) {
  // As with mem, in the absence of a key,
  // the system should use the sha256 hash
  // of the serialized object as a key.
  cb = cb || function() { };
  let actualKey;
  let actualGid = 'default';
  if (key == null) {
    actualKey = global.distribution.util.id.getID(obj);
  } else if (typeof key == 'string') {
    actualKey = key;
  } else if (typeof key == 'object' && ('key' in key) && ('gid' in key)) {
    actualGid = key.gid;
    if (key.key == null) {
      actualKey = global.distribution.util.id.getID(obj);
    } else {
      actualKey = key.key;
    }
  } else {
    cb(new Error('not valid key'));
    return;
  }

  if (typeof obj != 'object') {
    cb(new Error('invalid object'));
  }
  actualKey = actualKey.replace(/[^a-zA-Z0-9]/g, '');
  const dirKey = path.join(dir, actualGid);
  const fileName = path.join(dir, actualGid, actualKey);
  const data = global.distribution.util.serialize(obj);

  // write file contents to correct directory/gid

  if (!fs.existsSync(dirKey)) {
    fs.mkdirSync(dirKey);
  }

  fs.writeFileSync(fileName, data, {encoding: 'utf8'});
  setTimeout(() => {
    cb(null, obj);
  }, 1000);
};

store.del = function(key, cb) {
  // delete file
  cb = cb || function() { };
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

  actualKey = actualKey.replace(/[^a-zA-Z0-9]/g, '');
  const fileName = path.join(dir, actualGid, actualKey);

  if (fs.existsSync(fileName)) {
    fs.readFile(fileName, 'utf8', (err, data) => {
      if (err) {
        cb(err); return;
      };
      // if it does read file and deserealize value
      let obj = global.distribution.util.deserialize(data);

      fs.unlink(fileName, (err) => {
        if (err) {
          cb(err);
          return;
        }

        const dirKey = path.join(dir, actualGid);
        fs.readdir(dirKey, (err, files) => {
          if (err) {
            cb(err);
            return;
          }

          if (files.length === 0) {
            fs.rmdirSync(dirKey);
          }

          cb(null, obj);
        });
      });
    });
  } else {
    cb(new Error('key does not exist'));
  }
};

store.append = function(obj, key, cb) {
  cb = cb || function() { };
  let actualKey;
  let actualGid = 'default';
  if (key == null) {
    actualKey = global.distribution.util.id.getID(obj);
  } else if (typeof key == 'string') {
    actualKey = key;
  } else if (typeof key == 'object' && ('key' in key) && ('gid' in key)) {
    actualGid = key.gid;
    if (key.key == null) {
      actualKey = global.distribution.util.id.getID(obj);
    } else {
      actualKey = key.key;
    }
  } else {
    cb(new Error('not valid key'));
    return;
  }

  if (typeof obj != 'object') {
    cb(new Error('invalid object'));
  }

  actualKey = actualKey.replace(/[^a-zA-Z0-9]/g, '');
  const dirKey = path.join(dir, actualGid);
  const fileName = path.join(dir, actualGid, actualKey);

  if (!fs.existsSync(dirKey)) {
    fs.mkdirSync(dirKey);
  }

  if (fs.existsSync(fileName)) {
    let data = fs.readFileSync(fileName, {encoding: 'utf8'});
    data = global.distribution.util.deserialize(data);
    obj = [...obj, ...data];
  }

  fs.appendFileSync(fileName,
      global.distribution.util.serialize(obj),
      {encoding: 'utf8', flag: 'w'});
  cb(null);
};
module.exports = store;
