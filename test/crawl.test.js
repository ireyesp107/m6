global.nodeConfig = { ip: '127.0.0.1', port: 7291 };
const { Console } = require('console');
const distribution = require('../distribution');
const id = distribution.util.id;

const groupsTemplate = require('../distribution/all/groups');
const fs = require('fs');
const path = require('path');
const { store } = require('../distribution/local/local');
const { doesNotReject } = require('assert');

global.fetch = require('node-fetch');
const crawlerGroup = {};
const urlExtractGroup = {};
const stringMatchGroup = {};
const invertGroup = {};
const reverseLinkGroup = {};
const indexGroup = {};
const indexDataGroup = {};
const invertIndexGroup = {};



const compactTestGroup = {};
const memTestGroup = {};
const outTestGroup = {};
let uniqueKey = 0;
let iteration = 0;
jest.setTimeout(6000000)


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

const n1 = { ip: '127.0.0.1', port: 7922 };
const n2 = { ip: '127.0.0.1', port: 7923 };
const n3 = { ip: '127.0.0.1', port: 7924 };
const n4 = { ip: '127.0.0.1', port: 7925 };
const n5 = { ip: '127.0.0.1', port: 7926 };
const n6 = { ip: '127.0.0.1', port: 7927 };
const n7 = { ip: '127.0.0.1', port: 7928 };
const n8 = { ip: '127.0.0.1', port: 7929 };
const n9 = { ip: '127.0.0.1', port: 7920 };
// const n10 = { ip: '127.0.0.1', port: 8119 };
// const n11 = { ip: '127.0.0.1', port: 8120 };
// const n12 = { ip: '127.0.0.1', port: 8121 };


beforeAll((done) => {
    /* Stop the nodes if they are running */

    crawlerGroup[id.getSID(n1)] = n1;
    crawlerGroup[id.getSID(n2)] = n2;
    crawlerGroup[id.getSID(n3)] = n3;
    crawlerGroup[id.getSID(n4)] = n4;
    crawlerGroup[id.getSID(n5)] = n5;
    crawlerGroup[id.getSID(n6)] = n6;
    crawlerGroup[id.getSID(n7)] = n7;

    indexGroup[id.getSID(n1)] = n1;
    indexGroup[id.getSID(n2)] = n2;
    indexGroup[id.getSID(n3)] = n3;
    indexGroup[id.getSID(n4)] = n4;
    indexGroup[id.getSID(n5)] = n5;
    indexGroup[id.getSID(n6)] = n6;
    indexGroup[id.getSID(n7)] = n7;

    indexDataGroup[id.getSID(n1)] = n1;
    indexDataGroup[id.getSID(n2)] = n2;
    indexDataGroup[id.getSID(n3)] = n3;
    indexDataGroup[id.getSID(n4)] = n4;
    indexDataGroup[id.getSID(n5)] = n5;
    indexDataGroup[id.getSID(n6)] = n6;
    indexDataGroup[id.getSID(n7)] = n7;

    invertIndexGroup[id.getSID(n1)] = n1;
    invertIndexGroup[id.getSID(n2)] = n2;
    invertIndexGroup[id.getSID(n3)] = n3;
    invertIndexGroup[id.getSID(n4)] = n4;
    invertIndexGroup[id.getSID(n5)] = n5;
    invertIndexGroup[id.getSID(n6)] = n6;
    invertIndexGroup[id.getSID(n7)] = n7;
    // crawlerGroup[id.getSID(n8)] = n8;
    // crawlerGroup[id.getSID(n9)] = n9;
    // crawlerGroup[id.getSID(n10)] = n10;
    // crawlerGroup[id.getSID(n11)] = n11;
    // crawlerGroup[id.getSID(n12)] = n12;

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
                    distribution.local.status.spawn(n4, (e, v) => {
                        distribution.local.status.spawn(n5, (e, v) => {
                            distribution.local.status.spawn(n6, (e, v) => {
                                distribution.local.status.spawn(n7, (e, v) => {
                                    //cb();

                                    //  distribution.local.status.spawn(n8, (e, v) => {
                                    //      distribution.local.status.spawn(n9, (e, v) => {
                                    //          distribution.local.status.spawn(n10, (e, v) => {
                                    //              distribution.local.status.spawn(n11, (e, v) => {
                                    //                     distribution.local.status.spawn(n12, (e, v) => {
                                    cb();
                                    //                     });
                            //     });
                            // });
                        });
                    });
                });
            });
        });
                     });
                });
        //     });
        // });
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
                    const indexDataConfig = { gid: 'indexData' };
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
                                        //  remote.node = n8;
                                        //  distribution.local.comm.send([], remote, (e, v) => {
                                        //      remote.node = n9;
                                        //      distribution.local.comm.send([], remote, (e, v) => {
                                //                  remote.node = n10;
                                //                  distribution.local.comm.send([], remote, (e, v) => {
                                //                      remote.node = n11;
                                //                     distribution.local.comm.send([], remote, (e, v) => {
                                //                         remote.node = n12;

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
    //      });
    //  });
//  });
// });

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


test('all.mr:crawler-with-urlExtraction', (done) => {
    let m1 = async (key, value) => {
        const response = await global.fetch(value);
        const body = await response.text();

        // Extract URLs from the fetched HTML content using JSDOM
        function extractLinks(html, baseUrl) {
            const dom = new global.distribution.jsdom(html);
            const links = Array.from(dom.window.document.querySelectorAll('a'))
                .filter((link) => {
                    // Skipping links that have no href attribute or that start with '?'
                    const href = link.href;
                    if (href === '' || href.startsWith('?')) {
                        return false;
                    }
                    // Skipping the "Parent Directory" link
                    const text = link.textContent.trim();
                    if (text === 'Parent Directory'
                        || text === 'books.txt'
                        || text === 'donate-howto.txt'
                        || text === 'indextree.txt'
                        || text === 'retired/') {
                        return false;
                    }
                    return true;
                })
                .map((link) => {
                    const href = link.href;
                    // Resolve relative URLs to absolute URLs
                    if (href.startsWith('/')) {
                        return new URL(href, baseUrl).href;
                    } else {
                        return new URL(href, `${baseUrl}`).href;
                    }
                });
            return links;
        }

        const extractedUrls = extractLinks(body, value);
        let out = {};
        out[value] = { body: body, extractedUrls: extractedUrls };

        // Store the extracted URLs in the map function
        const uniqueUrls = [...new Set(extractedUrls)];

        const promises = uniqueUrls.map((url) => {
            let sanitizedKey = url.replace(/[^a-zA-Z0-9]/g, '');
            return new Promise((resolve, reject) => {
                distribution.crawler.store.put(url, sanitizedKey, (e, v) => {
                    if (e) {
                        reject(e);
                    } else {
                        resolve();
                    }
                });
            });
        });

        // Wait for all the promises to resolve before returning
        await Promise.all(promises);

        global.distribution.crawler.store.put(out[value], value, (e, v) => { });
        return out;
    };

    let r1 = (key, values) => {
        //console.log("the values for reduce ", key, values);
        let out = {};
        // Combine the extracted URLs from all the values
        let allExtractedUrls = values.flatMap((value) => value.extractedUrls);
        let uniqueUrls = [...new Set(allExtractedUrls)];
        out[key] = uniqueUrls;
        return out;
    };

    let dataset = [
        {
            '000': 'https://atlas.cs.brown.edu/data/gutenberg/6/0/0/',
        },
    ];

    //needs further testing
    let expected = [
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/6005/": [Array] },
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/6009/": [Array] },
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/9/": [Array] },
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/6002/": [Array] },
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/6004/": [Array] },
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/6001/": [Array] },
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/6007/": [Array] },
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/2/": [Array] },
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/6003/": [Array] },
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/6008/": [Array] },
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/6006/": [Array] },
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/8/": [Array] },
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/6/": [Array] },
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/5/": [Array] },
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/4/": [Array] },
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/3/": [Array] },
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/1/": [Array] },
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/6000/": [Array] },
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/7/": [Array] },
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/0/": [Array] }];

    /* Now we do the same thing but on the cluster */
    const doMapReduce = (cb) => {
        distribution.crawler.store.get(null, (e, v) => {
            try {
                //console.log(v);
                expect(v.length).toBe(dataset.length);
            } catch (e) {
                done(e);
            }

            distribution.crawler.mr.exec({ keys: v, map: m1, reduce: r1, iterations: 2 }, (e, v) => {
                try {
                    // expect(v).toEqual(expect.arrayContaining(expected));

                    const receivedKeys = v.map((item) => Object.keys(item)[0]);
                    const expectedKeys = expected.map((item) => Object.keys(item)[0]);

                    // Check if the received keys match the expected keys
                    expect(receivedKeys.sort()).toEqual(expectedKeys.sort());

                    // Check if each value in the received data is an array
                    const receivedData = v.map((item) => {
                        const key = Object.keys(item)[0];
                        const value = item[key];
                        return { [key]: Array.isArray(value) };
                    });

                    // Check if the received data matches the expected structure
                    expect(receivedData).toEqual(expect.arrayContaining(
                        expected.map((item) => {
                            const key = Object.keys(item)[0];
                            return { [key]: true };
                        })
                    ));

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

test('all.mr:crawler-start-urltxt', (done) => {
    let m1 = async (key, value) => {
        const response = await global.fetch(value);
        const body = await response.text();

        // Extract URLs from the fetched HTML content using JSDOM
        function extractLinks(html, baseUrl) {
            const dom = new global.distribution.jsdom(html);
            const links = Array.from(dom.window.document.querySelectorAll('a'))
                .filter((link) => {
                    // Skipping links that have no href attribute or that start with '?'
                    const href = link.href;
                    if (href === '' || href.startsWith('?')) {
                        return false;
                    }
                    // Skipping the "Parent Directory" link
                    const text = link.textContent.trim();
                    if (text === 'Parent Directory'
                        || text === 'books.txt'
                        || text === 'donate-howto.txt'
                        || text === 'indextree.txt'
                        || text === 'retired/') {
                        return false;
                    }
                    return true;
                })
                .map((link) => {
                    const href = link.href;
                    // Resolve relative URLs to absolute URLs
                    if (href.startsWith('/')) {
                        return new URL(href, baseUrl).href;
                    } else {
                        return new URL(href, `${baseUrl}`).href;
                    }
                });
            return links;
        }

        const extractedUrls = extractLinks(body, value);
        let out = {};
        out[value] = { body: body, extractedUrls: extractedUrls };

        global.distribution.crawler.store.put(out[value], value, (e, v) => { });
        return out;
    };

    let r1 = (key, values) => {
        let out = {};
        // Combine the extracted URLs from all the values
        let allExtractedUrls = values.flatMap((value) => value.extractedUrls);
        let uniqueUrls = [...new Set(allExtractedUrls)];
        out[key] = uniqueUrls;
        return out;
    };

    let dataset = [
        {
            '000': 'https://atlas.cs.brown.edu/data/gutenberg/6/0/0/',
        },
    ];

    let expected = [
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/": [Array] }
    ];

    /* Now we do the same thing but on the cluster */
    const doMapReduce = (cb) => {
        distribution.crawler.store.get(null, (e, v) => {
            try {
                console.log(v);
                expect(v.length).toBe(dataset.length);
            } catch (e) {
                done(e);
            }

            distribution.crawler.mr.exec({ keys: v, map: m1, reduce: r1, iterations: 1 }, (e, v) => {
                try {
                    console.log(v)
                    const receivedKeys = v.map((item) => Object.keys(item)[0]);
                    const expectedKeys = expected.map((item) => Object.keys(item)[0]);

                    // Check if the received keys match the expected keys
                    expect(receivedKeys.sort()).toEqual(expectedKeys.sort());

                    // Check if each value in the received data is an array
                    const receivedData = v.map((item) => {
                        const key = Object.keys(item)[0];
                        const value = item[key];
                        return { [key]: Array.isArray(value) };
                    });

                    // Check if the received data matches the expected structure
                    expect(receivedData).toEqual(expect.arrayContaining(
                        expected.map((item) => {
                            const key = Object.keys(item)[0];
                            return { [key]: true };
                        })
                    ));

                    // Save the output to urls.txt
                    const urlsToSave = v.flatMap((item) => {
                        const key = Object.keys(item)[0];
                        return item[key];
                    });

                    const urlsString = urlsToSave.join('\n');

                    fs.writeFile(path.join(__dirname, '../testFiles/urls.txt'), urlsString, (err) => {
                        if (err) {
                            console.error('Error writing to urls.txt:', err);
                            done(err);
                        } else {
                            console.log('URLs saved to urls.txt');

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
                        }
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





let finalUrls = []
let currentData = []

test('all.mr:crawler-homepage-urltxt-multiple rounds NOT USING FOR INTEGRATION', (done) => {
    // distribution.crawler.mem.put('https://atlas.cs.brown.edu/data/gutenberg/', "urls")
    fs.writeFile(path.join(__dirname, '../testFiles/urls.txt'), 'https://atlas.cs.brown.edu/data/gutenberg/', (err) => {
        if (err) {
            console.error('Error writing to urls.txt:', err);
            done(err);
        } else {
            console.log('Gutenberg homepage is saved');
        }

        function checkFileEmpty() {
            const data = fs.readFileSync(path.join(__dirname, '../testFiles/urls.txt'), 'utf8');
            return data;
        }
        let m1 = async (key, value) => {
            async function get_page(url) {
                return new Promise((resolve) => {
                    let data = '';
                    global.distribution.https.get(url, { rejectUnauthorized: false }, res => {
                        res.on('data', chunk => { data += chunk; });
                        res.on('end', () => { resolve(data); });
                    });
                });
            }

            try {
                if (!value) {
                    let err = {}
                    err['Brown'] = value
                    return err;
                }

                //const response = await global.fetch(value);

                //const body = await response.text();

                // Extract URLs from the fetched HTML content using JSDOM
                function extractLinks(html, baseUrl) {
                    const dom = new global.distribution.jsdom(html);
                    const links = Array.from(dom.window.document.querySelectorAll('a'))

                        .filter((link) => {
                            // Skipping links that have no href attribute or that start with '?'
                            const href = link.href;
                            if (href === '' || href.startsWith('?')) {
                                return false;
                            }
                            // Skipping the "Parent Directory" link
                            const text = link.textContent.trim();
                            if (text === 'Parent Directory'
                                || text === 'books.txt'
                                || text === 'donate-howto.txt'
                                || text === 'indextree.txt'
                                || text === 'retired/') {
                                return false;
                            }
                            return true;
                        })
                        .map((link) => {
                            const href = link.href;
                            // Resolve relative URLs to absolute URLs
                            if (href.startsWith('/')) {
                                return new URL(href, baseUrl).href;
                            } else {
                                return new URL(href, `${baseUrl}`).href;
                            }
                        });
                    return links;
                }

                const body = await get_page(value);

                //const lowerCaseBody = global.distribution.convert(body).toLowerCase();
                //console.log(lowerCaseBody)
                // Extract URLs from the content
                const extractedUrls = extractLinks(body, value).filter(url => url !== undefined);

                let out = {};
                out['Brown'] = extractedUrls ;
                //console.log(out);

                // let extractedUrls = extractLinks(body, value);
                // extractedUrls = extractedUrls.filter(url => url !== undefined);

                // let out = {};
                // out['Brown'] = {extractedUrls: extractedUrls };

                //global.distribution.crawler.store.put(out[value], value, (e, v) => { });
                return out;
            } catch (e) {
                console.error('Error fetching data for ' + value, e);

                return {}
            }
        };

        let r1 = (key, values) => {

            let out = {};
            // Combine the extracted URLs from all the values
            //values = values.filter(arrayValue => arrayValue.filter(value => 'extractedUrls' in value ).length > 0)
            //let allExtractedUrls = values.flatMap((value) => value.extractedUrls);
            //let allExtractedUrls = values.flatMap((value) => value ? value.extractedUrls : []);

            //let uniqueUrls = [...new Set(allExtractedUrls)];
            out[key] = values.flat();
            return out;
        };

        /* Now we do the same thing but on the cluster */
        const doMapReduce = (cb) => {

            let data = checkFileEmpty().trim()
            if (iteration > 3 || data === '') {
                done();
                return;
            }
            iteration++;

            // We send the dataset to the cluster
            let urlsDataset = ''
            urlsDataset = data.split('\n');
            //currentData = data.split('\n');

            //let cntr = 0
            //let roundCntr = 0;

            // split into subsets
            
            
            function subGroupExtraction(dataGroup){
                let subDataGroup = dataGroup.slice(0,500)
                let restDataGroup = dataGroup.slice(500)
                let toDelete = []
                let cntr = 0
                //base case
                if (subDataGroup.length < 250){
                    subDataGroup.forEach((o) => {
                        //console.log(o)
                        uniqueKey += 1
                        toDelete.push(uniqueKey)
                        distribution.crawler.store.put(o, uniqueKey.toString(), (e, v) => {
                            if (e) {
                                console.log(e);
                                done(e);
                            }
                            cntr++;
                            //roundCntr++;
                            // Once we are done, run the map reduce
                            // at the 100th url
                            if (cntr === subDataGroup.length) {

                             distribution.crawler.store.get(null, (e, v) => {

                                let filteredKeys = v.filter(element => !isNaN(element));
    
                                distribution.crawler.mr.exec({ keys: filteredKeys, map: m1, reduce: r1, iterations: 1}, (e, v) => {
                                    try {
                                        console.log(v)
    
                                        // Save the output to urls.txt
                                        const urlsToSave = v.flatMap((item) => {
                                            const key = Object.keys(item)[0];
                                            return item[key];
                                        });
    
                                        const urlsString = urlsToSave.join('\n');
    
                                        fs.appendFileSync(path.join(__dirname, '../testFiles/tempUrls.txt'), urlsString)

                                            //const data = 
                                            fs.readFile(path.join(__dirname, '../testFiles/tempUrls.txt'), 'utf8', (err, data) => {

                                            if (err) {
                                                console.error('Error writing to urls.txt:', err);
                                                done(err);
                                            } else {
                                                // let urlData = data
                                                // urlData = urlData.trim().split('\n'); 
                                                // console.log(urlData)

                                                fs.writeFile(path.join(__dirname, '../testFiles/tempUrls.txt'), '', (err) => {

                                                fs.writeFile(path.join(__dirname, '../testFiles/urls.txt'), data, (err) => {
                                                console.log('URLs saved to urls.txt');
                                                //index phase 1
    
                                                //index phase 2
    
                                                let delCntr = 0
                                                distribution.crawler.store.get(null, (e, filesToDelete) => {
    
                                                    filesToDelete = filesToDelete.filter(key => key !== 'tempResults')
                                                    filesToDelete.forEach((o) => {
                                                        distribution.crawler.store.del(o, (e, v) => {
                                                            delCntr++;
                                                            if (e) {
                                                                console.log(o)
                                                                console.log(e)
                                                            }
    
                                                            //delCntr++;
                                                            if (delCntr === filesToDelete.length) {
                                                                let remote = { service: 'store', method: 'del' }
                                                                distribution.crawler.comm.send([{ key: 'tempResults', gid: 'crawler' }], remote, (e, v) => {
                                                                    doMapReduce()
                                                                })
                                                            }
                                                        })
                                                    })
                                                })
                                            //})
                                            
                                            });
                                        });

                                            }
                                        //});
                                        });
                                    } catch (e) {
                                        console.log("here")
                                        done(e);
                                    }
                                });
                                //}
                            });
                        }
                    })
                })
                    //}
                }
                else{
                    subDataGroup.forEach((o) => {
                        //console.log(o)
                        uniqueKey += 1
                        toDelete.push(uniqueKey)
                        distribution.crawler.store.put(o, uniqueKey.toString(), (e, v) => {
                            if (e) {
                                console.log(e);
                                done(e);
                            }
                            cntr++;
                            //roundCntr++;
                            // Once we are done, run the map reduce
                            // at the 100th url
                            if (cntr === subDataGroup.length) {
                            distribution.crawler.store.get(null, (e, v) => {

                                let filteredKeys = v.filter(element => !isNaN(element));
    
                                distribution.crawler.mr.exec({ keys: filteredKeys, map: m1, reduce: r1, iterations: 1 }, (e, v) => {
                                    try {
                                       
                                        console.log(v)
                                        console.log(cntr)
    
                                        // Save the output to urls.txt
                                        const urlsToSave = v.flatMap((item) => {
                                            const key = Object.keys(item)[0];
                                            return item[key];
                                        });
    
                                        const urlsString = urlsToSave.join('\n');
    
                                        fs.appendFileSync(path.join(__dirname, '../testFiles/tempUrls.txt'), urlsString)//, (err) => {
                                            if (err) {
                                                console.error('Error writing to urls.txt:', err);
                                                done(err);
                                            } else {
                                                console.log('URLs saved to tempUrls.txt');
                                                //index phase 1
    
                                                //index phase 2
    
                                                let delCntr = 0
                                                distribution.crawler.store.get(null, (e, filesToDelete) => {
                                                    filesToDelete = filesToDelete.filter(key => key !== 'tempResults')
                                                    filesToDelete.forEach((o) => {
                                                        distribution.crawler.store.del(o, (e, v) => {
                                                            delCntr++;
                                                            if (e) {
                                                                console.log(o)
                                                                console.log(e)
                                                            }
                                                            //delCntr++;
                                                            if (delCntr === filesToDelete.length) {
                                                                let remote = { service: 'store', method: 'del' }
                                                                distribution.crawler.comm.send([{ key: 'tempResults', gid: 'crawler' }], remote, (e, v) => {
                                                                    subGroupExtraction(restDataGroup);
                                                                })
                                                            }
                                                        })
                                                    })
                                                })
    
                                            }
                                        //});
                                    } catch (e) {
                                        console.log("here")
                                        done(e);
                                    }
                                });
                                //}
                            });
                        }
                    });
                });

                        }
                    }
                subGroupExtraction(urlsDataset)
            };

        doMapReduce();

    });
});


test('Crawler we are going to use', (done) => {
    // distribution.crawler.mem.put('https://atlas.cs.brown.edu/data/gutenberg/', "urls")
    fs.writeFile(path.join(__dirname, '../testFiles/urls.txt'), 'https://atlas.cs.brown.edu/data/gutenberg/2/2/2/2/', (err) => {
        if (err) {
            console.error('Error writing to urls.txt:', err);
            done(err);
        } else {
            console.log('Gutenberg homepage is saved');
        }

        function checkFileEmpty() {
            const data = fs.readFileSync(path.join(__dirname, '../testFiles/urls.txt'), 'utf8');
            return data;
            //return fs.readFile(path.join(__dirname, '../testFiles/urls.txt'), 'utf8');
        }



        let m1 = async (key, value) => {
            async function get_page(url) {
                return new Promise((resolve) => {
                    let data = '';
                    global.distribution.https.get(url, { rejectUnauthorized: false }, res => {
                        res.on('data', chunk => { data += chunk; });
                        res.on('end', () => { resolve(data); });
                    });
                });
            }

            try {
                if (!value) {
                    let err = {}
                    err['Brown'] = value
                    return err;
                }

                //const response = await global.fetch(value);

                //const body = await response.text();

                // Extract URLs from the fetched HTML content using JSDOM
                function extractLinks(html, baseUrl) {
                    const dom = new global.distribution.jsdom(html);
                    const links = Array.from(dom.window.document.querySelectorAll('a'))

                        .filter((link) => {
                            // Skipping links that have no href attribute or that start with '?'
                            const href = link.href;
                            if (href === '' || href.startsWith('?')) {
                                return false;
                            }
                            // Skipping the "Parent Directory" link
                            const text = link.textContent.trim();
                            if (text === 'Parent Directory'
                                || text === 'books.txt'
                                || text === 'donate-howto.txt'
                                || text === 'indextree.txt'
                                || text === 'retired/') {
                                return false;
                            }
                            return true;
                        })
                        .map((link) => {
                            const href = link.href;
                            // Resolve relative URLs to absolute URLs
                            if (href.startsWith('/')) {
                                return new URL(href, baseUrl).href;
                            } else {
                                return new URL(href, `${baseUrl}`).href;
                            }
                        });
                    return links;
                }

                const body = await get_page(value);
                const stringBody = body

                //pageID to page content

                // async function put_Content() {
                //     return new Promise((resolve) => {
                //         global.distribution.index.store.put(body, key, (e, v) => {
                //             if (e) {
                //                 resolve('')
                //             } else {
                //                 resolve(v);
                //             }
                //         })
                //     })
                // }



                //const lowerCaseBody = global.distribution.convert(body).toLowerCase();
                //console.log(lowerCaseBody)
                // Extract URLs from the content
                const extractedUrls = extractLinks(body, value).filter(url => url !== undefined);
                // await put_Content();
                global.distribution.index.store.put([stringBody, value], key, (e, v) => { });

                let out = {};
                out['Brown'] = extractedUrls ;
                //console.log(out);

                // let extractedUrls = extractLinks(body, value);
                // extractedUrls = extractedUrls.filter(url => url !== undefined);

                // let out = {};
                // out['Brown'] = {extractedUrls: extractedUrls };

                //global.distribution.crawler.store.put(out[value], value, (e, v) => { });
                return out;
                
            } 
        catch (e) {
                console.error('Error fetching data for ' + value, e);

                return {}
            }
            
        
        };

        let r1 = (key, values) => {

            let out = {};
            // Combine the extracted URLs from all the values
            //values = values.filter(arrayValue => arrayValue.filter(value => 'extractedUrls' in value ).length > 0)
            //let allExtractedUrls = values.flatMap((value) => value.extractedUrls);
            //let allExtractedUrls = values.flatMap((value) => value ? value.extractedUrls : []);

            //let uniqueUrls = [...new Set(allExtractedUrls)];
            out[key] = values.flat();
            return out;
        };
        

        let mIndex = async (key, value) => {   
            // async function get_URL() {
            //     return new Promise((resolve) => {
            //         global.distribution.crawler.store.get(key, (e,v) => {
            //             if (e) {
            //                 resolve('')
            //             } else {
            //                 resolve(v);
            //             }
            //         })
            //     })
            // }

            function stemWords(text) {
                const tokenizer = new global.distribution.natural.WordTokenizer();
                const stemmedWords = tokenizer.tokenize(text).map(word => global.distribution.natural.PorterStemmer.stem(word).replace(/[^a-zA-Z0-9]/g, ''));
                return stemmedWords;
            }
            
            function countWords(words) {
                const counts = {};
    
                words.forEach(word => {
                    if (word in counts) {
                        counts[word] += 1; // Increment the count if the word exists
                    } else {
                        counts[word] = 1; // Initialize count if the word is new
                    }
                });
    
                const result = Object.keys(counts).map(key => ({
                    count: counts[key],
                    word: key
                }));
    
                return result;
            }
    
            function groupWordsByFirstLetter(wordCounts, url) {
                const grouped = {};
    
                wordCounts.forEach(item => {
                    // Extract the first letter of the word
                    const firstTwoLetters = item.word.slice(0, 2);
    
                    // Initialize the array if it does not exist
                    if (!grouped[firstTwoLetters]) {
                        grouped[firstTwoLetters] = [];
                    }
    
                    item['url'] = url;
                    
                    // Push the current item to the appropriate group
                    grouped[firstTwoLetters].push(item);
                });
    
                const groupedArray = Object.keys(grouped).map(letter => ({
                    [letter]: grouped[letter]
                }));
    
                return groupedArray;
            }
    
            const currURL = value[1];
            
            const text = global.distribution.convert(value[0]).toLowerCase()
            const stemmedWords = stemWords(text)
            const stopWordsFilePath = global.distribution.path.join(
                global.distribution.testFilesPath,
                'stopWords.txt');
            const stopWordsData = global.distribution.fs.readFileSync(stopWordsFilePath, { encoding: 'utf8' });
            const stopWords = stopWordsData.split('\n').map(word => word.trim());
            const filteredWords = stemmedWords.filter(word => !stopWords.includes(word) && isNaN(word));
    
            let wordCounts = countWords(filteredWords);
            let groupedWords = groupWordsByFirstLetter(wordCounts, currURL)
            let out = groupedWords;
    
            return out;
        };
    
        let rIndex = (key, values) => {
            // group all words in subset of the first two letters
            function groupWords(words) {
                const groupedLetters = {};
    
                words.forEach(wordItem => {
                    const word = wordItem.word;
    
                    if (!groupedLetters[word]) {
                        groupedLetters[word] = [];
                    }
    
                    if (Array.isArray(groupedLetters[word])) {
                        groupedLetters[word].push(wordItem)
                    }
                });
    
                return groupedLetters;
            }
    
    
            let out = {};
            //group values by words
            out[key] = groupWords(values.flat());
            return out;
        };

        /* Now we do the same thing but on the cluster */
        const doMapReduce = (cb) => {

            let data = checkFileEmpty().trim()
            if (iteration > 3 || data === '') {
                done();
                return;
            }
            iteration++;

            // We send the dataset to the cluster
            let urlsDataset = ''
            urlsDataset = data.split('\n');
            //currentData = data.split('\n');

            let cntr = 0
            let toDelete = []
            // append sync
            fs.appendFileSync(path.join(__dirname, '../testFiles/seenUrls.txt'), data +'\n')

            urlsDataset.forEach((o) => {
                //console.log(o)
                uniqueKey += 1
                toDelete.push(uniqueKey)
                distribution.crawler.store.put(o, uniqueKey.toString(), (e, v) => {
                    if (e) {
                        console.log(e);
                        done(e);
                    }
                    cntr++;
                    // Once we are done, run the map reduce
                    if (cntr === urlsDataset.length) {
                        distribution.crawler.store.get(null, (e, v) => {

                            let filteredKeys = v.filter(element => !isNaN(element));

                            distribution.crawler.mr.exec({ keys: filteredKeys, map: m1, reduce: r1, iterations: 1, memory:true}, (e, v) => {
                                try {
                                    console.log(v)
                                    const seenUrlData = fs.readFileSync(path.join(__dirname, '../testFiles/seenUrls.txt'), {encoding: 'utf8'});
                                    let seenUrls = ''
                                    seenUrls = new Set(seenUrlData.split('\n'));
                                    // Save the output to urls.txt
                                
                                    const urlsToSave = v.flatMap((item) => {
                                        const key = Object.keys(item)[0];
                                        return item[key];
                                    }).filter(url => !seenUrls.has(url));


                                    const urlsString = urlsToSave.join('\n');

                                    fs.writeFile(path.join(__dirname, '../testFiles/urls.txt'), urlsString, (err) => {
                                        if (err) {
                                            console.error('Error writing to urls.txt:', err);
                                            done(err);
                                        } else {
                                            console.log('URLs saved to urls.txt');
                                            // All current page content should be stored in IndexGroup
                                            // Key is pageId, value is page content

                                            // Run Index MapReduce
                                            distribution.index.mr.exec({keys: filteredKeys, map: mIndex, reduce: rIndex, memory: true}, (_,v) => {
                                                try {
                                                    // merge these kv pairs into disitrubted store
                                                    let numPuts = 0;
                                                    let numData = v.length;
                                                    v.forEach((firstTwoLetterObject) => {
                                                        let key = Object.keys(firstTwoLetterObject)[0];
                                                        let newBatch = Object.values(firstTwoLetterObject)[0]
                                                        let putBatch = newBatch
                                                        distribution.invertIndex.store.get(key, (e, existingBatch) => {
                                                            // merge existing Batch
                                                            if (!e) {
                                                                function mergeWordBatches(obj1, obj2) {
                                                                    const mergedResult = {};

                                                                    function mergeSort(arr1, arr2) {
                                                                        const mergedArray = arr1.concat(arr2);

                                                                        const uniqueMap = new Map();

                                                                        mergedArray.forEach(item => {
                                                                            uniqueMap.set(item.url, item);
                                                                        });

                                                                        const uniqueArray = Array.from(uniqueMap.values());

                                                                        uniqueArray.sort((a, b) => b.count - a.count);
                                                                        return uniqueArray;
                                                                    }

                                                                    const allKeys = new Set([...Object.keys(obj1), ...Object.keys(obj2)]);

                                                                    allKeys.forEach(key => {
                                                                        const arrayFromObj1 = obj1[key] || [];
                                                                        const arrayFromObj2 = obj2[key] || [];
                                                                        mergedResult[key] = mergeSort(arrayFromObj1, arrayFromObj2);
                                                                    });

                                                                    return mergedResult;
                                                                }

                                                                putBatch = mergeWordBatches(newBatch, existingBatch);
                                                            }

                                                            distribution.invertIndex.store.put(putBatch, key, (e, _) => {
                                                                if (e) {
                                                                    done(e)
                                                                } else {
                                                                    numPuts++;
                                                                    if (numPuts === numData) {
                                                                        // All kv pairs have been merged
                                                                        // delete all current keys (pageId) from Index & Crawl
                                                                        let delIds = 0
                                                                        distribution.crawler.store.get(null, (e, crawlerFilesToDelete) => {
                                                                                crawlerFilesToDelete = crawlerFilesToDelete.filter(key => key !== 'tempResults')
                                                                                crawlerFilesToDelete.forEach((o) => {
                                                                                    distribution.crawler.store.del(o, (e, v) => {
                                                                                        delIds++;
                                                                                        if (e) {
                                                                                            done(e)
                                                                                        }
                                                                                        
                                                                                        if (delIds === crawlerFilesToDelete.length) {
                                                                                            delIds = 0
                                                                                            distribution.index.store.get(null, (e, indexFilesToDelete) => {
                                                                                                indexFilesToDelete = indexFilesToDelete.filter(key => key !== 'tempResults')
                                                                                                indexFilesToDelete.forEach((o) => {
                                                                                                    distribution.index.store.del(o, (e, v) => {
                                                                                                        delIds++;
                                                                                                        if (e) {
                                                                                                            done(e)
                                                                                                        }
                
                                                                                                        if (delIds === indexFilesToDelete.length) {
                                                                                                            let remote = { service: 'mem', method: 'del' }
                                                                                                            distribution.crawler.comm.send([{ key: 'tempResults', gid: 'index' }], remote, (e,v) => {
                                                                                                                let remote = { service: 'mem', method: 'del' }
                                                                                                                distribution.index.comm.send([{ key: 'tempResults', gid: 'crawler' }], remote, (e, v) => {
                                                                                                                    doMapReduce();
                                                                                                                })
                                                                                                            })
                                                                                                        }
                                                                                                    })
                                                                                                })
                                                                                            })
                                                                                        }
                                                                                    })
                                                                                })
                                                                        })
                                                                    }
                                                                }
                                                            })
                                                        })
                                                    })
                                                } catch (e) {
                                                    done(e);
                                                }
                                            })
                                        }
                                    });
                                } catch (e) {
                                    console.log("here")
                                    done(e);
                                }
                            });
                        });

                    }
                });
            });
        };

        doMapReduce();
    });
});

test('Crawler merged!', (done) => {
    // distribution.crawler.mem.put('https://atlas.cs.brown.edu/data/gutenberg/', "urls")
    fs.writeFile(path.join(__dirname, '../testFiles/urls.txt'), 'https://atlas.cs.brown.edu/data/gutenberg/1/1/1/', (err) => {
        if (err) {
            console.error('Error writing to urls.txt:', err);
            done(err);
        } else {
            console.log('Gutenberg homepage is saved');
        }

        function checkFileEmpty() {
            const data = fs.readFileSync(path.join(__dirname, '../testFiles/urls.txt'), 'utf8');
            return data;
            //return fs.readFile(path.join(__dirname, '../testFiles/urls.txt'), 'utf8');
        }

        let m1 = async (key, value) => {
            async function get_page(url) {
                return new Promise((resolve) => {
                    let data = '';
                    global.distribution.https.get(url, { rejectUnauthorized: false }, res => {
                        res.on('data', chunk => { data += chunk; });
                        res.on('end', () => { resolve(data); });
                    });
                });
            }

            try {
                if (!value) {
                    let err = {}
                    err['Brown'] = value
                    return err;
                }

                function extractLinks(html, baseUrl) {
                    const dom = new global.distribution.jsdom(html);
                    const links = Array.from(dom.window.document.querySelectorAll('a'))

                        .filter((link) => {
                            // Skipping links that have no href attribute or that start with '?'
                            const href = link.href;
                            if (href === '' || href.startsWith('?')) {
                                return false;
                            }
                            // Skipping the "Parent Directory" link
                            const text = link.textContent.trim();
                            if (text === 'Parent Directory'
                                || text === 'books.txt'
                                || text === 'donate-howto.txt'
                                || text === 'indextree.txt'
                                || text === 'retired/') {
                                return false;
                            }
                            return true;
                        })
                        .map((link) => {
                            const href = link.href;
                            // Resolve relative URLs to absolute URLs
                            if (href.startsWith('/')) {
                                return new URL(href, baseUrl).href;
                            } else {
                                return new URL(href, `${baseUrl}`).href;
                            }
                        });
                    return links;
                }

                const body = await get_page(value);

                //console.log(lowerCaseBody)
                // Extract URLs from the content
                const extractedUrls = extractLinks(body, value).filter(url => url !== undefined).join('\n');

                const tempUrlsFilePath = global.distribution.path.join(
                    global.distribution.testFilesPath,
                    'tempUrls.txt');
                if (extractedUrls.length > 0) {
                    global.distribution.fs.appendFileSync(tempUrlsFilePath, extractedUrls + '\n')
                }

                function stemWords(text) {
                    const tokenizer = new global.distribution.natural.WordTokenizer();
                    const stemmedWords = tokenizer.tokenize(text).map(word => global.distribution.natural.PorterStemmer.stem(word).replace(/[^a-zA-Z0-9]/g, ''));
                    return stemmedWords;
                }

                function countWords(words) {
                    const counts = {};

                    words.forEach(word => {
                        if (word in counts) {
                            counts[word] += 1; // Increment the count if the word exists
                        } else {
                            counts[word] = 1; // Initialize count if the word is new
                        }
                    });

                    const result = Object.keys(counts).map(key => ({
                        count: counts[key],
                        word: key
                    }));

                    result.sort((a, b) => b.count - a.count);

                    const top100 = result.slice(0, 100);

                    return top100;
                }

                function groupWordsByFirstLetter(wordCounts, url) {
                    const grouped = {};

                    wordCounts.forEach(item => {
                        // Extract the first letter of the word
                        const firstTwoLetters = item.word.slice(0, 2);

                        // Initialize the array if it does not exist
                        if (!grouped[firstTwoLetters]) {
                            grouped[firstTwoLetters] = [];
                        }

                        item['url'] = url;

                        // Push the current item to the appropriate group
                        grouped[firstTwoLetters].push(item);
                    });

                    const groupedArray = Object.keys(grouped).map(letter => ({
                        [letter]: grouped[letter]
                    }));

                    return groupedArray;
                }

                const currURL = value;
                const text = global.distribution.convert(body).toLowerCase()
                const stemmedWords = stemWords(text)
                const stopWordsFilePath = global.distribution.path.join(
                    global.distribution.testFilesPath,
                    'stopWords.txt');
                const stopWordsData = global.distribution.fs.readFileSync(stopWordsFilePath, { encoding: 'utf8' });
                const stopWords = stopWordsData.split('\n').map(word => word.trim());
                const filteredWords = stemmedWords.filter(word => !stopWords.includes(word) && isNaN(word));

                let wordCounts = countWords(filteredWords);
                let groupedWords = groupWordsByFirstLetter(wordCounts, currURL)
                let out = groupedWords;

                console.log('done with: ' + key + ' ' + value + ', node: ' + JSON.stringify(global.nodeConfig))
                return out;
            }
            catch (e) {
                console.error('Error fetching data for ' + value, e);

                return {}
            }


        };

        let r1 = (key, values) => {
            function groupWords(words) {
                const groupedLetters = {};

                words.forEach(wordItem => {
                    if(wordItem && wordItem['word']) {
                    const word = wordItem.word;

                    if (!groupedLetters[word]) {
                        groupedLetters[word] = [];
                    }
                    

                    if (Array.isArray(groupedLetters[word])) {
                        delete wordItem.word;
                        groupedLetters[word].push(wordItem)
                    }
                    
                }
                });

                return groupedLetters;
            }

            let out = {};
            out[key] = groupWords(values.flat());
            return out;
        };


        /* Now we do the same thing but on the cluster */
        const doMapReduce = (cb) => {

            let data = checkFileEmpty().trim()
            if (iteration > 3 || data === '') {
                done();
                return;
            }
            iteration++;

            // We send the dataset to the cluster
            let urlsDataset = ''
            urlsDataset = data.split('\n');
            //currentData = data.split('\n');

            let cntr = 0
            let toDelete = []
            urlsDataset.forEach((o) => {
                //console.log(o)
                uniqueKey += 1
                toDelete.push(uniqueKey)
                distribution.crawler.store.put(o, uniqueKey.toString(), (e, v) => {
                    if (e) {
                        console.log(e);
                        done(e);
                    }
                    cntr++;
                    // Once we are done, run the map reduce
                    if (cntr === urlsDataset.length) {
                        distribution.crawler.store.get(null, (e, v) => {

                            let filteredKeys = v.filter(element => !isNaN(element));

                            distribution.crawler.mr.exec({ keys: filteredKeys, map: m1, reduce: r1, iterations: 1, memory:true }, (e, kvPairs) => {
                                try {
                                    // Save the output to urls.txt

                                    let urlsToSave = fs.readFileSync(path.join(__dirname, '../testFiles/tempUrls.txt'), { encoding: 'utf8' })
                                    fs.writeFile(path.join(__dirname, '../testFiles/tempUrls.txt'), '', (err) => {
                                        fs.writeFile(path.join(__dirname, '../testFiles/urls.txt'), urlsToSave, (err) => {
                                            if (err) {
                                                console.error('Error writing to urls.txt:', err);
                                                done(err);
                                            } else {
                                                let numPuts = 0;
                                                let numData = kvPairs.length
                                                kvPairs.forEach((firstTwoLetterObject) => {
                                                    let key = Object.keys(firstTwoLetterObject)[0];
                                                    let newBatch = Object.values(firstTwoLetterObject)[0]
                                                    let putBatch = newBatch
                                                    distribution.invertIndex.store.get(key, (e, existingBatch) => {
                                                        // merge existing Batch
                                                        if (!e) {
                                                            function mergeWordBatches(obj1, obj2) {
                                                                const mergedResult = {};
    
                                                                function mergeSort(arr1, arr2) {
                                                                    const mergedArray = arr1.concat(arr2);
    
                                                                    const uniqueMap = new Map();
    
                                                                    mergedArray.forEach(item => {
                                                                        uniqueMap.set(item.url, item);
                                                                    });
    
                                                                    const uniqueArray = Array.from(uniqueMap.values());
    
                                                                    uniqueArray.sort((a, b) => b.count - a.count);
                                                                    return uniqueArray;
                                                                }
    
                                                                const allKeys = new Set([...Object.keys(obj1), ...Object.keys(obj2)]);
    
                                                                allKeys.forEach(key => {
                                                                    const arrayFromObj1 = obj1[key] || [];
                                                                    const arrayFromObj2 = obj2[key] || [];
                                                                    mergedResult[key] = mergeSort(arrayFromObj1, arrayFromObj2);
                                                                });
    
                                                                return mergedResult;
                                                            }
    
                                                            putBatch = mergeWordBatches(newBatch, existingBatch);
                                                        }
    
                                                        distribution.invertIndex.store.put(putBatch, key, (e, _) => {
                                                            if (e) {
                                                                done(e)
                                                            } else {
                                                                numPuts++;
                                                                if (numPuts === numData) {
                                                                    // All kv pairs have been merged
                                                                    // delete all current keys (pageId) from Index & Crawl
                                                                    let delIds = 0
                                                                    distribution.crawler.store.get(null, (e, crawlerFilesToDelete) => {
                                                                        crawlerFilesToDelete = crawlerFilesToDelete.filter(key => key !== 'tempResults')
                                                                        crawlerFilesToDelete.forEach((o) => {
                                                                            distribution.crawler.store.del(o, (e, v) => {
                                                                                delIds++;
                                                                                if (e) {
                                                                                    done(e)
                                                                                }
    
                                                                                if (delIds === crawlerFilesToDelete.length) {
                                                                                    let remote = { service: 'mem', method: 'del' }
                                                                                    distribution.crawler.comm.send([{ key: 'tempResults', gid: 'crawler' }], remote, (e, v) => {
                                                                                        doMapReduce();
                                                                                    })
                                                                                }
                                                                            })
                                                                        })
                                                                    })
                                                                }
                                                            }
                                                        })
                                                    })
                                                })
                                            }
                                        });

                                    })
                                } catch (e) {
                                    console.log("here")
                                    done(e);
                                }
                            });
                        });

                    }
                });
            });
        };

        doMapReduce();
    });
})

test('Crawler merged! 2', (done) => {
    // distribution.crawler.mem.put('https://atlas.cs.brown.edu/data/gutenberg/', "urls")
    fs.writeFile(path.join(__dirname, '../testFiles/urls.txt'), 'https://atlas.cs.brown.edu/data/gutenberg/1/1/2/', (err) => {
        if (err) {
            console.error('Error writing to urls.txt:', err);
            done(err);
        } else {
            console.log('Gutenberg homepage is saved');
        }

        function checkFileEmpty() {
            const data = fs.readFileSync(path.join(__dirname, '../testFiles/urls.txt'), 'utf8');
            return data;
            //return fs.readFile(path.join(__dirname, '../testFiles/urls.txt'), 'utf8');
        }

        let m1 = async (key, value) => {
            async function get_page(url) {
                return new Promise((resolve) => {
                    let data = '';
                    global.distribution.https.get(url, { rejectUnauthorized: false }, res => {
                        res.on('data', chunk => { data += chunk; });
                        res.on('end', () => { resolve(data); });
                    });
                });
            }

            try {
                if (!value) {
                    let err = {}
                    err['Brown'] = value
                    return err;
                }

                function extractLinks(html, baseUrl) {
                    const dom = new global.distribution.jsdom(html);
                    const links = Array.from(dom.window.document.querySelectorAll('a'))

                        .filter((link) => {
                            // Skipping links that have no href attribute or that start with '?'
                            const href = link.href;
                            if (href === '' || href.startsWith('?')) {
                                return false;
                            }
                            // Skipping the "Parent Directory" link
                            const text = link.textContent.trim();
                            if (text === 'Parent Directory'
                                || text === 'books.txt'
                                || text === 'donate-howto.txt'
                                || text === 'indextree.txt'
                                || text === 'retired/') {
                                return false;
                            }
                            return true;
                        })
                        .map((link) => {
                            const href = link.href;
                            // Resolve relative URLs to absolute URLs
                            if (href.startsWith('/')) {
                                return new URL(href, baseUrl).href;
                            } else {
                                return new URL(href, `${baseUrl}`).href;
                            }
                        });
                    return links;
                }

                const body = await get_page(value);

                //console.log(lowerCaseBody)
                // Extract URLs from the content
                const extractedUrls = extractLinks(body, value).filter(url => url !== undefined).join('\n');

                const tempUrlsFilePath = global.distribution.path.join(
                    global.distribution.testFilesPath,
                    'tempUrls.txt');
                if (extractedUrls.length > 0) {
                    global.distribution.fs.appendFileSync(tempUrlsFilePath, extractedUrls + '\n')
                }

                function stemWords(text) {
                    const tokenizer = new global.distribution.natural.WordTokenizer();
                    const stemmedWords = tokenizer.tokenize(text).map(word => global.distribution.natural.PorterStemmer.stem(word).replace(/[^a-zA-Z0-9]/g, ''));
                    return stemmedWords;
                }

                function countWords(words) {
                    const counts = {};

                    words.forEach(word => {
                        if (word in counts) {
                            counts[word] += 1; // Increment the count if the word exists
                        } else {
                            counts[word] = 1; // Initialize count if the word is new
                        }
                    });

                    const result = Object.keys(counts).map(key => ({
                        count: counts[key],
                        word: key
                    }));

                    result.sort((a, b) => b.count - a.count);

                    const top100 = result.slice(0, 100);

                    return top100;
                }

                function groupWordsByFirstLetter(wordCounts, url) {
                    const grouped = {};

                    wordCounts.forEach(item => {
                        // Extract the first letter of the word
                        const firstTwoLetters = item.word.slice(0, 2);

                        // Initialize the array if it does not exist
                        if (!grouped[firstTwoLetters]) {
                            grouped[firstTwoLetters] = [];
                        }

                        item['url'] = url;

                        // Push the current item to the appropriate group
                        grouped[firstTwoLetters].push(item);
                    });

                    const groupedArray = Object.keys(grouped).map(letter => ({
                        [letter]: grouped[letter]
                    }));

                    return groupedArray;
                }

                const currURL = value;
                const text = global.distribution.convert(body).toLowerCase()
                const stemmedWords = stemWords(text)
                const stopWordsFilePath = global.distribution.path.join(
                    global.distribution.testFilesPath,
                    'stopWords.txt');
                const stopWordsData = global.distribution.fs.readFileSync(stopWordsFilePath, { encoding: 'utf8' });
                const stopWords = stopWordsData.split('\n').map(word => word.trim());
                const filteredWords = stemmedWords.filter(word => !stopWords.includes(word) && isNaN(word));

                let wordCounts = countWords(filteredWords);
                let groupedWords = groupWordsByFirstLetter(wordCounts, currURL)
                let out = groupedWords;

                console.log('done with: ' + key + ' ' + value + ', node: ' + JSON.stringify(global.nodeConfig))
                return out;
            }
            catch (e) {
                console.error('Error fetching data for ' + value, e);

                return {}
            }


        };

        let r1 = (key, values) => {
            function groupWords(words) {
                const groupedLetters = {};

                words.forEach(wordItem => {
                    if(wordItem && wordItem['word']) {
                    const word = wordItem.word;

                    if (!groupedLetters[word]) {
                        groupedLetters[word] = [];
                    }
                    

                    if (Array.isArray(groupedLetters[word])) {
                        delete wordItem.word;
                        groupedLetters[word].push(wordItem)
                    }
                    
                }
                });

                return groupedLetters;
            }

            let out = {};
            out[key] = groupWords(values.flat());
            return out;
        };


        /* Now we do the same thing but on the cluster */
        const doMapReduce = (cb) => {

            let data = checkFileEmpty().trim()
            if (iteration > 3 || data === '') {
                done();
                return;
            }
            iteration++;

            // We send the dataset to the cluster
            let urlsDataset = ''
            urlsDataset = data.split('\n');
            //currentData = data.split('\n');

            let cntr = 0
            let toDelete = []
            urlsDataset.forEach((o) => {
                //console.log(o)
                uniqueKey += 1
                toDelete.push(uniqueKey)
                distribution.crawler.store.put(o, uniqueKey.toString(), (e, v) => {
                    if (e) {
                        console.log(e);
                        done(e);
                    }
                    cntr++;
                    // Once we are done, run the map reduce
                    if (cntr === urlsDataset.length) {
                        distribution.crawler.store.get(null, (e, v) => {

                            let filteredKeys = v.filter(element => !isNaN(element));

                            distribution.crawler.mr.exec({ keys: filteredKeys, map: m1, reduce: r1, iterations: 1, memory:true }, (e, kvPairs) => {
                                try {
                                    // Save the output to urls.txt

                                    let urlsToSave = fs.readFileSync(path.join(__dirname, '../testFiles/tempUrls.txt'), { encoding: 'utf8' })
                                    fs.writeFile(path.join(__dirname, '../testFiles/tempUrls.txt'), '', (err) => {
                                        fs.writeFile(path.join(__dirname, '../testFiles/urls.txt'), urlsToSave, (err) => {
                                            if (err) {
                                                console.error('Error writing to urls.txt:', err);
                                                done(err);
                                            } else {
                                                let numPuts = 0;
                                                let numData = kvPairs.length
                                                kvPairs.forEach((firstTwoLetterObject) => {
                                                    let key = Object.keys(firstTwoLetterObject)[0];
                                                    let newBatch = Object.values(firstTwoLetterObject)[0]
                                                    let putBatch = newBatch
                                                    distribution.invertIndex.store.get(key, (e, existingBatch) => {
                                                        // merge existing Batch
                                                        if (!e) {
                                                            function mergeWordBatches(obj1, obj2) {
                                                                const mergedResult = {};
    
                                                                function mergeSort(arr1, arr2) {
                                                                    const mergedArray = arr1.concat(arr2);
    
                                                                    const uniqueMap = new Map();
    
                                                                    mergedArray.forEach(item => {
                                                                        uniqueMap.set(item.url, item);
                                                                    });
    
                                                                    const uniqueArray = Array.from(uniqueMap.values());
    
                                                                    uniqueArray.sort((a, b) => b.count - a.count);
                                                                    return uniqueArray;
                                                                }
    
                                                                const allKeys = new Set([...Object.keys(obj1), ...Object.keys(obj2)]);
    
                                                                allKeys.forEach(key => {
                                                                    const arrayFromObj1 = obj1[key] || [];
                                                                    const arrayFromObj2 = obj2[key] || [];
                                                                    mergedResult[key] = mergeSort(arrayFromObj1, arrayFromObj2);
                                                                });
    
                                                                return mergedResult;
                                                            }
    
                                                            putBatch = mergeWordBatches(newBatch, existingBatch);
                                                        }
    
                                                        distribution.invertIndex.store.put(putBatch, key, (e, _) => {
                                                            if (e) {
                                                                done(e)
                                                            } else {
                                                                numPuts++;
                                                                if (numPuts === numData) {
                                                                    // All kv pairs have been merged
                                                                    // delete all current keys (pageId) from Index & Crawl
                                                                    let delIds = 0
                                                                    distribution.crawler.store.get(null, (e, crawlerFilesToDelete) => {
                                                                        crawlerFilesToDelete = crawlerFilesToDelete.filter(key => key !== 'tempResults')
                                                                        crawlerFilesToDelete.forEach((o) => {
                                                                            distribution.crawler.store.del(o, (e, v) => {
                                                                                delIds++;
                                                                                if (e) {
                                                                                    done(e)
                                                                                }
    
                                                                                if (delIds === crawlerFilesToDelete.length) {
                                                                                    let remote = { service: 'mem', method: 'del' }
                                                                                    distribution.crawler.comm.send([{ key: 'tempResults', gid: 'crawler' }], remote, (e, v) => {
                                                                                        doMapReduce();
                                                                                    })
                                                                                }
                                                                            })
                                                                        })
                                                                    })
                                                                }
                                                            }
                                                        })
                                                    })
                                                })
                                            }
                                        });

                                    })
                                } catch (e) {
                                    console.log("here")
                                    done(e);
                                }
                            });
                        });

                    }
                });
            });
        };

        doMapReduce();
    });
})