// Internally, every service method converts the primary key to a
// key identifier (KID) by applying a sha256 method on the primary key.
// NIDs and KIDs must draw from the same identifier space to be comparable —
// so both of them should  be sha256 values.
let mem = (config) => {
  let context = {};

  context.gid = config.gid || 'all'; // contains a property named gid
  context.hash = config.hash || global.distribution.util.id.naiveHash;

  return {
    get: (key, cb) => {
      if (key == null) {
        global.distribution[context.gid].comm.send(
            [{key: key, gid: context.gid}],
            {service: 'mem', method: 'get'}, (e, v) => {
              if (e == null) {
                e = {};
              }
              cb(e, Object.values(v).flat());
            });
      } else {
        global.distribution.local.groups.get(context.gid, (e, v) => {
          if (e != null) {
            cb(e);
            return;
          }

          let allNodes = Object.values(v);
          let nids = allNodes.map((node) =>
            global.distribution.util.id.getNID(node));

          // apply hash
          const kid = global.distribution.util.id.getID(key);
          const chosenNid = context.hash(kid, nids);
          const chosenNode = allNodes.filter((node)=>
            global.distribution.util.id.getNID(node) === chosenNid)[0];

          // convert key into object with key and gid
          let keyObject = {key: key, gid: context.gid};

          // send local.comm.send
          let remote = {service: 'mem', method: 'get', node: chosenNode};
          global.distribution.local.comm.send([keyObject], remote, (e, v) => {
            cb(e, v);
          });
        });
      }
    },
    put: (obj, key, cb) => {
      if (key == null) {
        global.distribution[context.gid].comm.send(
            [obj, {key: key, gid: context.gid}],
            {service: 'mem', method: 'put'}, (e, v) => {
              cb(e, Object.values(v).flat());
            });
      } else {
        global.distribution.local.groups.get(context.gid, (e, v) => {
          if (e != null) {
            cb(e);
            return;
          }

          let allNodes = Object.values(v);
          let nids = allNodes.map((node) =>
            global.distribution.util.id.getNID(node));

          // apply hash
          const kid = global.distribution.util.id.getID(key);
          const chosenNid = context.hash(kid, nids);
          const chosenNode = allNodes.filter((node)=>
            global.distribution.util.id.getNID(node) === chosenNid)[0];

          // convert key into object with key and gid
          let keyObject = {key: key, gid: context.gid};

          // send local.comm.send
          let remote = {service: 'mem', method: 'put', node: chosenNode};
          global.distribution.local.comm.send(
              [obj, keyObject], remote, (e, v) => {
                cb(e, v);
              });
        });
      }
    },
    del: (key, cb) => {
      global.distribution.local.groups.get(context.gid, (e, v) => {
        if (e != null) {
          cb(e);
          return;
        }

        let allNodes = Object.values(v);
        let nids = allNodes.map((node) =>
          global.distribution.util.id.getNID(node));

        // apply hash
        const kid = global.distribution.util.id.getID(key);
        const chosenNid = context.hash(kid, nids);
        const chosenNode = allNodes.filter((node)=>
          global.distribution.util.id.getNID(node) === chosenNid)[0];

        // convert key into object with key and gid
        let keyObject = {key: key, gid: context.gid};

        // send local.comm.send
        let remote = {service: 'mem', method: 'del', node: chosenNode};
        global.distribution.local.comm.send([keyObject], remote, (e, v) => {
          cb(e, v);
        });
      });
    },
    reconf: (prevState, cb) => {
      // it goes through the list of object keys available
      // in the service instance; it achieves this using a
      // get with a null key, as described earlier.

      let prevNodes = Object.values(prevState);
      let prevNids = prevNodes.map((node) =>
        global.distribution.util.id.getNID(node));

      global.distribution[context.gid].mem.get(null, (e, v) => {
        if (Object.keys(e).length !== 0) {
          cb(e);
          return;
        } else {
          // v is a list of all keys
          let allKeys = new Set(Object.values(v).flat());
          global.distribution.local.groups.get(context.gid, (e, nodes) => {
            if (e != null) {
              cb(e);
            } else {
              let newNodes = Object.values(nodes);
              let newNids = newNodes.map((node) =>
                global.distribution.util.id.getNID(node));

              // check for relocate
              let toRelocate = new Map();

              allKeys.forEach((key) => {
                const kid = global.distribution.util.id.getID(key);

                const prevNid = context.hash(kid, prevNids);
                const newNid = context.hash(kid, newNids);

                const prevNode = prevNodes.filter((node) =>
                  global.distribution.util.id.getNID(node) === prevNid)[0];
                const newNode = newNodes.filter((node) =>
                  global.distribution.util.id.getNID(node) === newNid)[0];

                toRelocate.set(key,
                    {
                      prevNode: prevNode,
                      newNode: newNode,
                    });
              });

              // throw new Error(JSON.stringify([...toRelocate]))
              let relocated = 0;
              let numRelocate = toRelocate.size;
              toRelocate.forEach((value, key) => {
                global.distribution.local.comm.send([
                  {key: key, gid: context.gid}],
                {service: 'mem', method: 'get', node: value.prevNode},
                (e, v) => {
                  if (e != null) {
                    cb(e);
                    return;
                  } else {
                    let obj = v;
                    global.distribution.local.comm.send(
                        [{key: key, gid: context.gid}],
                        {
                          service: 'mem',
                          method: 'del',
                          node: value.prevNode,
                        },
                        (e, v) => {
                          if (e != null) {
                            cb(e);
                            return;
                          } else {
                            global.distribution.local.comm.send(
                                [obj, {key: key, gid: context.gid}],
                                {
                                  service: 'mem',
                                  method: 'put',
                                  node: value.newNode,
                                },
                                (e, v) => {
                                  if (e != null) {
                                    cb(e);
                                    return;
                                  } else {
                                    relocated += 1;
                                    if (numRelocate === relocated) {
                                      cb(null, null);
                                      return;
                                    }
                                  }
                                });
                          }
                        });
                  }
                });
              });
              // cb(null, null)
            }
          });
        }
      });
    },
    append: (obj, key, cb) => {
      if (key == null) {
        global.distribution[context.gid].comm.send(
            [obj, {key: key, gid: context.gid}],
            {service: 'mem', method: 'put'}, (e, v) => {
              cb(e, Object.values(v).flat());
            });
      } else {
        global.distribution.local.groups.get(context.gid, (e, v) => {
          if (e != null) {
            cb(e);
            return;
          }

          let allNodes = Object.values(v);
          let nids = allNodes.map((node) =>
            global.distribution.util.id.getNID(node));

          // apply hash
          const kid = global.distribution.util.id.getID(key);
          const chosenNid = global.distribution.util.id
              .consistentHash(kid, nids);
          const chosenNode = allNodes.filter((node)=>
            global.distribution.util.id.getNID(node) === chosenNid)[0];

          // convert key into object with key and gid
          let keyObject = {key: key, gid: context.gid};

          // send local.comm.send
          let remote = {service: 'mem', method: 'append', node: chosenNode};
          global.distribution.local.comm.send(
              [obj, keyObject], remote, (e, v) => {
                cb(e, v);
              });
        });
      }
    },
  };
};
module.exports = mem;
