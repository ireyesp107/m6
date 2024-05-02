global.nodeConfig = { ip: '172.31.0.115', port: 8070 };
const { Console } = require('console');
const distribution = require('../distribution');
const id = distribution.util.id;
const fs = require('fs');
const path = require('path');
const groupsFilePath = path.join(__dirname, '..', 'distribution', 'util', 'groups.txt');
jest.setTimeout(600000)

//groups added dynamically 
const urlExtractGroup = {};
const stringMatchGroup = {};
const invertGroup = {};
const reverseLinkGroup = {};
const compactTestGroup = {};
const memTestGroup = {};
const outTestGroup = {};

const groupsTemplate = require('../distribution/all/groups');

global.fetch = require('node-fetch');

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

const n1 = { ip: '127.0.0.1', port: 8110 };
const n2 = { ip: '127.0.0.1', port: 8111 };
const n3 = { ip: '127.0.0.1', port: 8112 };

function getNodesFromFile() {
    const groupsData = fs.readFileSync(groupsFilePath, 'utf8');
    const nodes = {};

    groupsData.split('\n').forEach((line) => {
        if (line.trim() !== '') {
            const [nodeId, nodeData] = line.split(': ');
            nodes[nodeId] = JSON.parse(nodeData);
        }
    });

    return nodes;
}

const crawlerGroup = getNodesFromFile();

beforeAll((done) => {
    /* Stop the nodes if they are running */
    distribution.node.start((server) => {
        localServer = server;

        const crawlerConfig = { gid: 'crawler', hash: global.distribution.util.id.consistentHash };
        groupsTemplate(crawlerConfig).put(
            crawlerConfig, crawlerGroup, (e, v) => {
            });
    });
});

afterAll((done) => {
    const nodes = Object.values(crawlerGroup);
    const stopNode = (index) => {
        if (index >= nodes.length) {
            localServer.close();
            done();
            return;
        }
        const remote = {
            service: 'status',
            method: 'stop',
            node: nodes[index]
        };
        distribution.local.comm.send([], remote, (e, v) => {
            stopNode(index + 1);
        });
    };
    stopNode(0);
});

test('all.mr:crawler!', (done) => {
    let m1 = async (key, value) => {
        const response = await global.fetch(value);
        const body = await response.text();

        let out = {};
        out[value] = body;

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
            '000': 'https://atlas.cs.brown.edu/data/gutenberg/6/0/0/',
        },
    ];

    let expected = [
        {
            'https://atlas.cs.brown.edu/data/gutenberg/6/0/0/': 'stored',
        },
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

            distribution.crawler.mr.exec({ keys: v, map: m1, reduce: r1 }, (e, v) => {
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
            }
        });
    });
});
