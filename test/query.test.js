global.nodeConfig = {ip: '127.0.0.1', port: 8070};
const distribution = require('../distribution');
const id = distribution.util.id;

const groupsTemplate = require('../distribution/all/groups');

global.fetch = require('node-fetch');
const queryGroup = {};

let localServer = null;

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
  
    stringMatchGroup[id.getSID(n1)] = n1;
    stringMatchGroup[id.getSID(n2)] = n2;
    stringMatchGroup[id.getSID(n3)] = n3;
  
    invertGroup[id.getSID(n1)] = n1;
    invertGroup[id.getSID(n2)] = n2;
    invertGroup[id.getSID(n3)] = n3;
  
    reverseLinkGroup[id.getSID(n1)] = n1;
    reverseLinkGroup[id.getSID(n2)] = n2;
    reverseLinkGroup[id.getSID(n3)] = n3;
  
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
            done();
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

test('all.mr:query', (done) => {
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
    //query is trigram
    // convert trigram to twoletterquery 
    distribution.query.store.get(twoLetterQuery, (e,v) => {
        // v is a batch object

        let results = v[santizedQuery]
        console.log(results)



    })
  
  })