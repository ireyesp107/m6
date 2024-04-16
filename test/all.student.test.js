global.nodeConfig = {ip: '127.0.0.1', port: 8070};
const distribution = require('../distribution');
const id = distribution.util.id;

const groupsTemplate = require('../distribution/all/groups');

global.fetch = require('node-fetch');
const crawlerGroup = {};
const urlExtractGroup = {};
const invertGroup = {};
const compactTestGroup = {};
const memTestGroup = {};
const outTestGroup = {};

/*
   This hack is necessary since we can not
   gracefully stop the local listening node.
   The process that node is
   running in is the actual jest process
*/
let localServer = null;

/*
    The local node will be the orchestrator.
*/

const n1 = {ip: '127.0.0.1', port: 8110};
const n2 = {ip: '127.0.0.1', port: 8111};
const n3 = {ip: '127.0.0.1', port: 8112};

beforeAll((done) => {
  /* Stop the nodes if they are running */

  crawlerGroup[id.getSID(n1)] = n1;
  crawlerGroup[id.getSID(n2)] = n2;
  crawlerGroup[id.getSID(n3)] = n3;

  urlExtractGroup[id.getSID(n1)] = n1;
  urlExtractGroup[id.getSID(n2)] = n2;
  urlExtractGroup[id.getSID(n3)] = n3;

  invertGroup[id.getSID(n1)] = n1;
  invertGroup[id.getSID(n2)] = n2;
  invertGroup[id.getSID(n3)] = n3;

  compactTestGroup[id.getSID(n1)] = n1;
  compactTestGroup[id.getSID(n2)] = n2;
  compactTestGroup[id.getSID(n3)] = n3;

  memTestGroup[id.getSID(n1)] = n1;
  memTestGroup[id.getSID(n2)] = n2;
  memTestGroup[id.getSID(n3)] = n3;

  outTestGroup[id.getSID(n1)] = n1;
  outTestGroup[id.getSID(n2)] = n2;
  outTestGroup[id.getSID(n3)] = n3;

  const startNodes = (cb) => {
    distribution.local.status.spawn(n1, (e, v) => {
      distribution.local.status.spawn(n2, (e, v) => {
        distribution.local.status.spawn(n3, (e, v) => {
          cb();
        });
      });
    });
  };

  distribution.node.start((server) => {
    localServer = server;

    const crawlerConfig = {gid: 'crawler'};
    startNodes(() => {
      groupsTemplate(crawlerConfig).put(
          crawlerConfig, crawlerGroup, (e, v) => {
            const urlExtractConfig = {gid: 'urlExtract'};
            groupsTemplate(urlExtractConfig).put(
                urlExtractConfig, urlExtractGroup, (e, v) => {
                  const invertConfig = {gid: 'invert'};
                  groupsTemplate(invertConfig).put(
                      invertConfig, invertGroup, (e, v) => {
                        const compactTestConfig = {gid: 'compactTest'};
                        groupsTemplate(compactTestConfig).put(
                            compactTestConfig, compactTestGroup, (e, v) => {
                              const memTestConfig = {gid: 'memTest'};
                              groupsTemplate(memTestConfig).put(
                                  memTestConfig, memTestGroup, (e, v) => {
                                    const outTestConfig = {gid: 'outTest'};
                                    groupsTemplate(outTestConfig).put(
                                        outTestConfig, outTestGroup, (e, v) => {
                                          done();
                                        });
                                  });
                            });
                      });
                });
          });
    });
  });
});

afterAll((done) => {
  let remote = {service: 'status', method: 'stop'};
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        localServer.close();
        done();
      });
    });
  });
});

test('all.mr:crawler', (done) => {
  let m1 = async (key, value) => {
    const response = await global.fetch(value);
    const body = await response.text();

    let out = {};
    out[value] = global.distribution.convert(body);

    global.distribution.crawler.store.put(out[value], value, (e, v) => { });
    return out;
  };

  let r1 = (key, values) => {
    let out = {};
    out[key] = 'stored';
    return out;
  };

  let dataset = [
    {
      '000': 'https://pvac.xyz/',
    },
    {
      '111': 'https://cv.btxx.org/',
    },
    {
      '222': 'https://rideout.net/',
    },
    {
      '333': 'https://t0.vc/',
    },
  ];

  let expected = [
    {
      'https://pvac.xyz/': 'stored',
    },
    {'https://cv.btxx.org/': 'stored'},
    {'https://rideout.net/': 'stored'},
    {'https://t0.vc/': 'stored'},
  ];

  /* Sanity check: map and reduce locally */
  // sanityCheck(m1, r1, dataset, expected, done);

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.crawler.store.get(null, (e, v) => {
      try {
        console.log(v);
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.crawler.mr.exec({keys: v, map: m1, reduce: r1}, (e, v) => {
        try {
          expect(v).toEqual(expect.arrayContaining(expected));

          let numStored = 0;
          v.forEach((url) => {
            const urlKey = Object.keys(url)[0];
            global.distribution.crawler.store.get(urlKey, (e, v) => {
              if (v) {
                numStored += 1;
                if (numStored === expected.length) {
                  done();
                }
              }
            });
          });
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let cntr = 0;

  // We send the dataset to the cluster
  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.crawler.store.put(value, key, (e, v) => {
      if (e) {
        console.log(e);
        console.log(value);
        console.log(key);
      }
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

test('all.mr:urlExtract', (done) => {
  let m2 = (key, value) => {
    // map each page to url {page-id: url}
    const filePath = global.distribution.path.join(
        global.distribution.testFilesPath,
        value + '.txt');
    const data = global.distribution.fs.readFileSync(
        filePath, {encoding: 'utf8'});

    const hrefRegex = /<a\s+(?:[^>]*?\s+)?href=["']([^"']*)["'][^>]*>/gi;
    const out = [];
    let match;
    while ((match = hrefRegex.exec(data)) !== null) {
      let o = {};
      o[value] = match[1];
      out.push(o);
    }
    return out;
  };

  let r2 = (key, values) => {
    // check for dupies, store urls
    const uniques =
      values.filter((value, index, self) => self.indexOf(value) === index);

    let out = {};
    out[key] = uniques;

    global.distribution.urlExtract.store.put(uniques, key, (e, v) => { });

    return out;
  };

  let dataset = [
    {'000': 'd0'},
    {'111': 'd1'},
  ];

  let expected = [
    {'d0': ['level_2a/index.html', 'level_2b/index.html']},
    {'d1': ['https://btxx.org', 'https://git.btxx.org']},
  ];

  /* Sanity check: map and reduce locally */
  // sanityCheck(m2, r2, dataset, expected, done);

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.urlExtract.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.urlExtract.mr.exec(
          {keys: v, map: m2, reduce: r2},
          (e, v) => {
            try {
              expect(v).toEqual(expect.arrayContaining(expected));
              done();
            } catch (e) {
              done(e);
            }
          });
    });
  };

  let cntr = 0;

  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.urlExtract.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

test('all.mr:invert', (done) => {
  let m3 = (key, value) => {
    // map each term to url {page-id: url}
    const filePath = global.distribution.path.join(
        global.distribution.testFilesPath,
        value + '.txt');
    const data = global.distribution.fs.readFileSync(
        filePath, {encoding: 'utf8'});
    let words = data.split(/(\s+)/).filter((e) => e !== ' ');
    let out = [];
    words.forEach((w) => {
      let o = {};
      o[w] = value;
      out.push(o);
    });
    return out;
  };

  let r3 = (key, values) => {
    const uniques =
      values.filter((value, index, self) => self.indexOf(value) === index);

    let out = {};
    out[key] = uniques.sort();
    return out;
  };

  let dataset = [
    {'000': 'l1'},
    {'111': 'l2'},
    {'222': 'l3'},
    {'33': 'l4'},
  ];

  let expected = [
    {It: ['l1']}, {was: ['l1', 'l2', 'l3', 'l4']},
    {the: ['l1', 'l2', 'l3', 'l4']}, {best: ['l1']},
    {of: ['l1', 'l2', 'l3', 'l4']}, {'times,': ['l1']},
    {it: ['l1', 'l2', 'l3', 'l4']}, {worst: ['l1']},
    {age: ['l2']}, {'wisdom,': ['l2']},
    {'foolishness,': ['l2']}, {epoch: ['l3']},
    {'belief,': ['l3']}, {'incredulity,': ['l3']},
    {season: ['l4']}, {'Light,': ['l4']},
    {'Darkness,': ['l4']},
  ];

  const doMapReduce = (cb) => {
    distribution.invert.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      const startTime = performance.now();
      distribution.invert.mr.exec(
          {keys: v, map: m3, reduce: r3},
          (e, v) => {
            try {
              const endTime = performance.now();
              const timeTaken = endTime - startTime;
              console.log('Time taken for exec with mem off:',
                  timeTaken, 'milliseconds');
              expect(v).toEqual(expect.arrayContaining(expected));
              done();
            } catch (e) {
              done(e);
            }
          });
    });
  };

  let cntr = 0;

  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.invert.store.put(value, key, (e, v) => {
      cntr++;
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

test('all.mr:compact-test', (done) => {
  let m3 = (key, value) => {
    // map each term to url {page-id: url}
    const filePath = global.distribution.path.join(
        global.distribution.testFilesPath,
        value + '.txt');
    const data = global.distribution.fs.readFileSync(
        filePath, {encoding: 'utf8'});
    let words = data.split(/(\s+)/).filter((e) => e !== ' ');
    let out = [];
    words.forEach((w) => {
      let o = {};
      o[w] = value;
      out.push(o);
    });
    return out;
  };

  let r3 = (key, values) => {
    const uniques =
      values.filter((value, index, self) => self.indexOf(value) === index);

    let out = {};
    out[key] = uniques.sort();
    return out;
  };

  let dataset = [
    {'000': 'l1'},
    {'111': 'l2'},
    {'222': 'l3'},
    {'33': 'l4'},
  ];

  let expected = [
    {It: ['l1']}, {was: ['l1', 'l2', 'l3', 'l4']},
    {the: ['l1', 'l2', 'l3', 'l4']}, {best: ['l1']},
    {of: ['l1', 'l2', 'l3', 'l4']}, {'times,': ['l1']},
    {it: ['l1', 'l2', 'l3', 'l4']}, {worst: ['l1']},
    {age: ['l2']}, {'wisdom,': ['l2']},
    {'foolishness,': ['l2']}, {epoch: ['l3']},
    {'belief,': ['l3']}, {'incredulity,': ['l3']},
    {season: ['l4']}, {'Light,': ['l4']},
    {'Darkness,': ['l4']},
  ];

  const doMapReduce = (cb) => {
    distribution.compactTest.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.compactTest.mr.exec(
          {keys: v, map: m3, reduce: r3,
            compact: r3}, (e, v) => {
            try {
              expect(v).toEqual(expect.arrayContaining(expected));
              done();
            } catch (e) {
              done(e);
            }
          });
    });
  };

  let cntr = 0;

  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.compactTest.store.put(value, key, (e, v) => {
      cntr++;
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

test('all.mr:mem-test', (done) => {
  let m3 = (key, value) => {
    // map each term to url {page-id: url}
    const filePath = global.distribution.path.join(
        global.distribution.testFilesPath,
        value + '.txt');
    const data = global.distribution.fs.readFileSync(
        filePath, {encoding: 'utf8'});
    let words = data.split(/(\s+)/).filter((e) => e !== ' ');
    let out = [];
    words.forEach((w) => {
      let o = {};
      o[w] = value;
      out.push(o);
    });
    return out;
  };

  let r3 = (key, values) => {
    const uniques =
      values.filter((value, index, self) => self.indexOf(value) === index);

    let out = {};
    out[key] = uniques.sort();
    return out;
  };

  let dataset = [
    {'000': 'l1'},
    {'111': 'l2'},
    {'222': 'l3'},
    {'33': 'l4'},
  ];

  let expected = [
    {It: ['l1']}, {was: ['l1', 'l2', 'l3', 'l4']},
    {the: ['l1', 'l2', 'l3', 'l4']}, {best: ['l1']},
    {of: ['l1', 'l2', 'l3', 'l4']}, {'times,': ['l1']},
    {it: ['l1', 'l2', 'l3', 'l4']}, {worst: ['l1']},
    {age: ['l2']}, {'wisdom,': ['l2']},
    {'foolishness,': ['l2']}, {epoch: ['l3']},
    {'belief,': ['l3']}, {'incredulity,': ['l3']},
    {season: ['l4']}, {'Light,': ['l4']},
    {'Darkness,': ['l4']},
  ];

  const doMapReduce = (cb) => {
    distribution.memTest.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      const startTime = performance.now();
      distribution.memTest.mr.exec(
          {keys: v, map: m3, reduce: r3,
            memory: true}, (e, v) => {
            try {
              const endTime = performance.now();
              const timeTaken = endTime - startTime;
              console.log('Time taken for exec with mem:',
                  timeTaken, 'milliseconds');
              expect(v).toEqual(expect.arrayContaining(expected));
              done();
            } catch (e) {
              done(e);
            }
          });
    });
  };

  let cntr = 0;

  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.memTest.store.put(value, key, (e, v) => {
      cntr++;
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

test('all.mr:out-test', (done) => {
  let m3 = (key, value) => {
    // map each term to url {page-id: url}
    const filePath = global.distribution.path.join(
        global.distribution.testFilesPath,
        value + '.txt');
    const data = global.distribution.fs.readFileSync(
        filePath, {encoding: 'utf8'});
    let words = data.split(/(\s+)/).filter((e) => e !== ' ');
    let out = [];
    words.forEach((w) => {
      let o = {};
      o[w] = value;
      out.push(o);
    });
    return out;
  };

  let r3 = (key, values) => {
    const uniques =
      values.filter((value, index, self) => self.indexOf(value) === index);

    let out = {};
    out[key] = uniques.sort();
    return out;
  };

  let dataset = [
    {'000': 'l1'},
    {'111': 'l2'},
    {'222': 'l3'},
    {'33': 'l4'},
  ];

  let expected = [
    {It: ['l1']}, {was: ['l1', 'l2', 'l3', 'l4']},
    {the: ['l1', 'l2', 'l3', 'l4']}, {best: ['l1']},
    {of: ['l1', 'l2', 'l3', 'l4']}, {'times,': ['l1']},
    {it: ['l1', 'l2', 'l3', 'l4']}, {worst: ['l1']},
    {age: ['l2']}, {'wisdom,': ['l2']},
    {'foolishness,': ['l2']}, {epoch: ['l3']},
    {'belief,': ['l3']}, {'incredulity,': ['l3']},
    {season: ['l4']}, {'Light,': ['l4']},
    {'Darkness,': ['l4']},
  ];

  const doMapReduce = (cb) => {
    distribution.outTest.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.outTest.mr.exec(
          {keys: v, map: m3, reduce: r3,
            memory: true, out: 'outTest'},
          (e, v) => {
            try {
              expect(v).toEqual(expect.arrayContaining(expected));
              done();
            } catch (e) {
              done(e);
            }
          });
    });
  };

  let cntr = 0;

  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.outTest.store.put(value, key, (e, v) => {
      cntr++;
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});
