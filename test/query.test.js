global.nodeConfig = { ip: '127.0.0.1', port: 7591 };
const { Console } = require('console');
const distribution = require('../distribution');
const id = distribution.util.id;

const groupsTemplate = require('../distribution/all/groups');
const fs = require('fs');
const path = require('path');
const { store } = require('../distribution/local/local');
const { doesNotReject } = require('assert');

global.fetch = require('node-fetch');
const invertIndexGroup = {};
jest.setTimeout(6000000)

let localServer = null;
const n1 = { ip: '127.0.0.1', port: 7932 };
const n2 = { ip: '127.0.0.1', port: 7933 };
const n3 = { ip: '127.0.0.1', port: 7934 };
const n4 = { ip: '127.0.0.1', port: 7935 };
const n5 = { ip: '127.0.0.1', port: 7936 };
const n6 = { ip: '127.0.0.1', port: 7937 };
const n7 = { ip: '127.0.0.1', port: 7938 };
const n8 = { ip: '127.0.0.1', port: 7939 };
const n9 = { ip: '127.0.0.1', port: 7930 };

beforeAll((done) => {
  invertIndexGroup[id.getSID(n1)] = n1;
  invertIndexGroup[id.getSID(n2)] = n2;
  invertIndexGroup[id.getSID(n3)] = n3;
  invertIndexGroup[id.getSID(n4)] = n4;
  invertIndexGroup[id.getSID(n5)] = n5;
  invertIndexGroup[id.getSID(n6)] = n6;
  invertIndexGroup[id.getSID(n7)] = n7;

  const startNodes = (cb) => {
      distribution.local.status.spawn(n1, (e, v) => {
          distribution.local.status.spawn(n2, (e, v) => {
              distribution.local.status.spawn(n3, (e, v) => {
                  distribution.local.status.spawn(n4, (e, v) => {
                      distribution.local.status.spawn(n5, (e, v) => {
                          distribution.local.status.spawn(n6, (e, v) => {
                              distribution.local.status.spawn(n7, (e, v) => {
                                  cb();
                      });
                  });
              });
          });
      });
                   });
              });
  };

  distribution.node.start((server) => {
      localServer = server;

      const crawlerConfig = { gid: 'crawler' };
      startNodes(() => {
          groupsTemplate(crawlerConfig).put(
              crawlerConfig, crawlerGroup, (e, v) => {
                  const indexConfig = { gid: 'index'};
                  groupsTemplate(indexConfig).put(
                      indexConfig, indexGroup, (e, v) => {
                  const indexDataConfig = { gid: 'indexData', hash: id.consistentHash };
                  groupsTemplate(indexDataConfig).put(
                      indexDataConfig, indexDataGroup, (e, v) => {
                  const invertIndexConfig = { gid: 'invertIndex', hash: id.consistentHash };
                  groupsTemplate(invertIndexConfig).put(
                      invertIndexConfig, invertIndexGroup, (e, v) => {
                  const urlExtractConfig = { gid: 'urlExtract' };
                  groupsTemplate(urlExtractConfig).put(
                      urlExtractConfig, urlExtractGroup, (e, v) => {
                          const invertConfig = { gid: 'invert' };
                          groupsTemplate(invertConfig).put(
                              invertConfig, invertGroup, (e, v) => {
                                  const compactTestConfig = { gid: 'compactTest' };
                                  groupsTemplate(compactTestConfig).put(
                                      compactTestConfig, compactTestGroup,
                                      (e, v) => {
                                          const memTestConfig = { gid: 'memTest' };
                                          groupsTemplate(memTestConfig).put(
                                              memTestConfig,
                                              memTestGroup,
                                              (e, v) => {
                                                  const outTestConfig =
                                                      { gid: 'outTest' };
                                                  groupsTemplate(outTestConfig).put(
                                                      outTestConfig,
                                                      outTestGroup,
                                                      (e, v) => {
                                                          const stringMatchConfig =
                                                              { gid: 'stringMatch' };
                                                          groupsTemplate(stringMatchConfig).put(
                                                              stringMatchConfig,
                                                              stringMatchGroup,
                                                              (e, v) => {
                                                                  const reverseLinkConfig =
                                                                      { gid: 'reverseLink' };
                                                                  groupsTemplate(
                                                                      reverseLinkConfig).put(
                                                                          reverseLinkConfig,
                                                                          reverseLinkGroup,
                                                                          (e, v) => {
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
          });
          });
      });
  });
});

afterAll((done) => {
  let remote = { service: 'status', method: 'stop' };
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n2;
      distribution.local.comm.send([], remote, (e, v) => {
          remote.node = n3;
          distribution.local.comm.send([], remote, (e, v) => {
              remote.node = n4;
              distribution.local.comm.send([], remote, (e, v) => {
                  remote.node = n5;
                  distribution.local.comm.send([], remote, (e, v) => {
                      remote.node = n6;
                      distribution.local.comm.send([], remote, (e, v) => {
                          remote.node = n7;
                          distribution.local.comm.send([], remote, (e, v) => {
                              distribution.local.comm.send([], remote, (e, v) => {
                                  localServer.close();
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

function findInBatch(queryWord, batch) {
  if (batch[queryWord]) {
      return batch[queryWord]
  } else {
      return 'no results found'
  }
}

function processQuery(query){
  const tokenizer = new global.distribution.natural.WordTokenizer();
  const stemmedWords = tokenizer.tokenize(text).map(word => global.distribution.natural.PorterStemmer.stem(word).replace(/[^a-zA-Z0-9]/g, ''));
  const stopWordsFilePath = global.distribution.path.join(
    global.distribution.testFilesPath,
    'stopWords.txt');
  const stopWordsData = global.distribution.fs.readFileSync(stopWordsFilePath, { encoding: 'utf8' });
  const stopWords = stopWordsData.split('\n').map(word => word.trim());
  const filteredWords = stemmedWords.filter(word => !stopWords.includes(word) && isNaN(word));

  if (filteredWords.length > 0) {
    return filteredWords[0];
  } else {
    return '';
  }
}

test('query basic', (done) => {
    let query = "project"
    let processedQuery = processQuery(query)
    let queryBatch = query.slice(0,2)

    distribution.invertIndex.store.get(queryBatch, (e,v) => {
        console.log(v)
        expect(e).toBeFalsy();
        let result = findInBatch(processedQuery, v)
        console.log(result)

        expect(Array.isArray(result)).toBe(true);
        done();
    })
})