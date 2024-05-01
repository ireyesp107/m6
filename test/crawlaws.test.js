global.nodeConfig = { ip: '127.0.0.1', port: 8070 };
const { Console } = require('console');
const distribution = require('../distribution');
const id = distribution.util.id;

//groups added dynamically 
const {
    server,
    crawlerGroup,
    urlExtractGroup,
    stringMatchGroup,
    invertGroup,
    reverseLinkGroup,
    compactTestGroup,
    memTestGroup,
    outTestGroup
} = require('../api/groupBuilder');

const groupsTemplate = require('../distribution/all/groups');
const fs = require('fs');
const path = require('path');

global.fetch = require('node-fetch');
const crawlerGroup = {};
const urlExtractGroup = {};
const stringMatchGroup = {};
const invertGroup = {};
const reverseLinkGroup = {};

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

const n1 = { ip: '127.0.0.1', port: 8110 };
const n2 = { ip: '127.0.0.1', port: 8111 };
const n3 = { ip: '127.0.0.1', port: 8112 };

beforeAll((done) => {
    /* Stop the nodes if they are running */

    // Wait for the worker nodes to join the groups
    const checkNodesJoined = () => {
        if (
            Object.keys(crawlerGroup).length === 10 &&
            Object.keys(urlExtractGroup).length === 10 &&
            Object.keys(stringMatchGroup).length === 10 &&
            Object.keys(invertGroup).length === 10 &&
            Object.keys(reverseLinkGroup).length === 10 &&
            Object.keys(compactTestGroup).length === 10 &&
            Object.keys(memTestGroup).length === 10 &&
            Object.keys(outTestGroup).length === 10
        ) {
            done();
        } else {
            setTimeout(checkNodesJoined, 1000);
        }
    };

    checkNodesJoined();

    distribution.node.start((server) => {
        localServer = server;

        const crawlerConfig = { gid: 'crawler' };
        groupsTemplate(crawlerConfig).put(
            crawlerConfig, crawlerGroup, (e, v) => {
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

afterAll((done) => {
    let remote = { service: 'status', method: 'stop' };
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
                doMapReduce();
            }
        });
    });
});
