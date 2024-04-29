const mr = function (config) {
  let context = {};
  context.gid = config.gid || 'all';

  return {
    exec: (configuration, callback) => {
      let iterationCount = 0;
      const iterations = configuration.iterations || 1;

      const runMapReduce = () => {
        let mrService = {};
        let typeStorage;
        if (configuration.memory) {
          typeStorage = 'mem';
        } else {
          typeStorage = 'store';
        }

        configuration.typeStorage = typeStorage;

        mrService.map = function (config, context, cb) {
          global.distribution.local.store.get(
            { key: null, gid: context.gid },
            (e, v) => {
              if (!v) {
                cb(null, {});
                return;
              }

              //get the intersection between the keys and the configuration.keys
              const intersection = config.keys.filter(key => v.includes(key));

              const numKeys = intersection.length;
              let tempValues = {};
              let processedKeys = 0;

              intersection.forEach((localKey) => {
                global.distribution.local.store.get(
                  { key: localKey, gid: context.gid },
                  (e, localValue) => {
                    if (config.map.constructor ===
                      async function () { }.constructor) {
                      Promise.resolve(
                        config.map(localKey, localValue))
                        .then((response) => {
                          if (response && response.skip) {
                            // If the response is { skip: true }, skip processing
                            processedKeys++;
                          } else {
                            tempValues[localKey] = response;
                          }

                          if (processedKeys + Object.keys(tempValues).length === numKeys) {
                            global.distribution
                              .local[config.typeStorage].put(
                                tempValues,
                                {
                                  key: 'tempResults',
                                  gid: context.gid
                                },
                                (e, v) => {
                                  cb(null, tempValues);
                                });
                          }
                        });
                    } else {
                      // tempValues[localKey] = config.map(localKey, localValue);
                      // if (numKeys === Object.keys(tempValues).length) {
                      //   global.distribution.local[config.typeStorage].put(
                      //     tempValues,
                      //     {
                      //       key: 'tempResults',
                      //       gid: context.gid
                      //     },
                      //     (e, v) => {
                      //       cb(null, tempValues);
                      //     });
                      // }
                      const response = config.map(localKey, localValue);
                      if (response && response.skip) {
                        // If the response is { skip: true }, skip processing
                        processedKeys++;
                      } else {
                        tempValues[localKey] = response;
                      }

                      if (processedKeys + Object.keys(tempValues).length === numKeys) {
                        global.distribution.local[config.typeStorage].put(
                          tempValues,
                          { key: 'tempResults', gid: context.gid },
                          (e, v) => {
                            cb(null, tempValues);
                          }
                        );
                      }
                    }
                  });
              });
            });
        };



        mrService.shuffle = function (config, context, cb) {
          // TODO: add optimizations
          global.distribution.local[config.typeStorage].get(
            { key: 'tempResults', gid: context.gid },
            (e, v) => {
              if (e !== null) {
                cb(null);
                return;
              }

              const groupedPairs = new Map();
              Object.values(v).forEach((tempObject) => {
                if (Array.isArray(tempObject)) {
                  tempObject.forEach((tempPair) => {
                    let tempPairKey = Object.keys(tempPair)[0];
                    if (!groupedPairs.has(tempPairKey)) {
                      groupedPairs.set(tempPairKey, []);
                    }
                    groupedPairs.get(tempPairKey).push(tempPair[tempPairKey]);
                  });
                } else {
                  let tempKey = Object.keys(tempObject)[0];
                  if (!groupedPairs.has(tempKey)) {
                    groupedPairs.set(tempKey, []);
                  }
                  groupedPairs.get(tempKey).push(tempObject[tempKey]);
                }
              });

              let numGroupedPairs = groupedPairs.size;
              let numFinished = 0;

              for (let groupKey of groupedPairs.keys()) {
                let currentGroup = groupedPairs.get(groupKey);

                if (config.compact) {
                  const compactReturn = config.compact(groupKey, currentGroup);
                  if (Array.isArray(Object.values(compactReturn)[0])) {
                    currentGroup = Object.values(compactReturn)[0];
                  } else {
                    currentGroup = Object.values(compactReturn);
                  }
                }

                groupKey += config.serviceName;
                global.distribution[context.gid][config.typeStorage].append(
                  currentGroup,
                  groupKey, (e, v) => {
                    numFinished += 1;
                    if (numFinished === numGroupedPairs) {
                      if (e) {
                        cb(e);
                      } else {
                        cb(null, Array.from(groupedPairs.keys()));
                      }
                      return;
                    }
                  });
              }
            });
        };

        mrService.reduce = function (config, context, cb) {
          global.distribution.local[config.typeStorage].get(
            { key: null, gid: context.gid },
            (e, v) => {
              if (!v) {
                cb(null, []);
                return;
              }
              const intersection = [];
              for (let key of v) {
                if (key.endsWith(config.serviceName)) {
                  intersection.push(key);
                }
              }

              const reducePairs = [];
              if (intersection.length > 0) {
                intersection.forEach((myKey) => {
                  global.distribution.local[config.typeStorage].get(
                    { key: myKey, gid: context.gid },
                    (e, values) => {
                      // TODO: add distrubuted persistance
                      const kvPair = config.reduce(
                        myKey.slice(0, -config.serviceName.length),
                        values);
                      reducePairs.push(kvPair);

                      if (config.out) {
                        global.distribution[config.out].store.put(
                          Object.values(kvPair)[0],
                          'output' + Object.keys(kvPair)[0],
                          (e, v) => {
                            if (reducePairs.length ===
                              intersection.length) {
                              cb(null, null);
                            }
                          });
                      } else {
                        if (reducePairs.length === intersection.length) {
                          cb(null, reducePairs);
                        }
                      }
                    });
                });
              } else {
                cb(null, intersection);
              }
            });
        };

        let mrServiceName = `mr-${context.gid}`;
        configuration.serviceName = context.gid;
        let remote = { service: mrServiceName, method: 'register' };

        global.distribution[context.gid].comm.send(
          [mrService],
          remote,
          (e, v) => {
            //console.log(e);
            remote.method = 'map';
            global.distribution[context.gid].comm.send(
              [configuration, context],
              remote,
              (e, v) => {
                //console.log(JSON.stringify(v));
                remote.method = 'shuffle';
                global.distribution[context.gid].comm.send(
                  [configuration, context],
                  remote,
                  (e, shuffleValues) => {
                    console.log(e);
                    const allKeys =
                      new Set(Object.values(shuffleValues).flat());
                    if (allKeys.has(undefined)) {
                      allKeys.delete(undefined);
                    }
                    remote.method = 'reduce';
                    global.distribution[context.gid].comm.send(
                      [configuration, context],
                      remote,
                      (e, reduceValue) => {
                        if (configuration.iterations > 1) {
                          // Extract the keys from the reduceValue
                          const extractedValues = Object.values(reduceValue).flatMap(group => {
                            return group.flatMap(obj => {
                              return Object.values(obj).flatMap(arr => {
                                return arr.map(url => url.replace(/[^a-zA-Z0-9]/g, ''));
                              });
                            });
                          });

                          // Update the configuration.keys with the extracted keys
                          configuration.keys = extractedValues;
                        }

                        remote.method = 'deregister';
                        global.distribution[context.gid].comm.send(
                          [mrService],
                          remote,
                          (e, v) => {
                            console.log(e);
                            let returnArray = [];

                            if (configuration.out) {
                              allKeys.forEach((actualKey) => {
                                let outputKey = actualKey;

                                if (!configuration.memory) {
                                  outputKey =
                                    actualKey
                                      .replace(/[^a-zA-Z0-9]/g, '');
                                }

                                outputKey = 'output' + outputKey;
                                global.distribution[configuration.out]
                                  .store.get(outputKey,
                                    (e, outputValue) => {
                                      if (e) {
                                        console
                                          .log(
                                            e +
                                            '-' + outputKey);
                                      }
                                      let kv = {};
                                      kv[actualKey] = outputValue;
                                      returnArray.push(kv);
                                      if (returnArray.length ===
                                        allKeys.size) {
                                        callback(null, returnArray);
                                      }
                                    });
                              });
                            } else {
                              iterationCount++;
                              if (iterationCount < iterations) {
                                runMapReduce();
                              } else {
                                // callback(null, returnArray);

                                const allPairs =
                                  Object.values(reduceValue).flat();

                                const allFileKeys =
                                  allPairs.map((pair) =>
                                    Object.keys(pair)[0]
                                      .replace(/[^a-zA-Z0-9]/g, ''));

                                for (let key of allKeys) {
                                  const fileKey =
                                    key.replace(/[^a-zA-Z0-9]/g, '');
                                  const foundPair =
                                    allPairs[allFileKeys.indexOf(fileKey)];
                                  const actualPair = {};
                                  actualPair[key] =
                                    Object.values(foundPair)[0];
                                  returnArray.push(actualPair);
                                }
                                callback(null, returnArray);
                              }
                            }
                          });
                      });
                  });
              });
          });
      };

      runMapReduce();
    }
  };
};

module.exports = mr;
