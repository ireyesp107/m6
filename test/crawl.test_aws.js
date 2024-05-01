global.nodeConfig = { ip: '127.0.0.1', port: 8070 };
const { Console } = require('console');
const distribution = require('../distribution');
const id = distribution.util.id;

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

test('all.mr:crawler-url-extraction-twice', (done) => {
    let parts = [];
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


    let expected = [
        { "https://atlas.cs.brown.edu/data/gutenberg/6/0/0/": [Array] }
    ];

    let expected2 = [
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

    let keys = [];
    /* Now we do the same thing but on the cluster */
    const doMapReduce = (cb) => {
        // get the values from urls.txt and make those the keys
        fs.readFile(path.join(__dirname, '../testFiles/urls.txt'), 'utf8', (err, data) => {
            if (err) {
                console.error('Error reading urls.txt:', err);
                done(err);
            } else {
                keys = data.split('\n').map((key) => key.replace(/[^a-zA-Z0-9]/g, ''));

                // place all of the keys in the distributed storage, then i need to remove them at the end of the run
                distribution.crawler.mr.exec({ keys: keys, map: m1, reduce: r1 }, (e, v) => {
                    try {
                        // Save the output to urls.txt
                        const urlsToSave = v.flatMap((item) => {
                            const key = Object.keys(item)[0];
                            return item[key];
                        });

                        const urlsString = urlsToSave.join('\n');
                        console.log("the values r ", v)

                        fs.writeFile(path.join(__dirname, '../testFiles/urls.txt'), urlsString, { flag: 'w' }, (err) => {
                            if (err) {
                                console.error('Error writing to urls.txt:', err);
                                done(err);
                            } else {
                                let numStored = 0;

                                v.forEach((url) => {
                                    const urlKey = Object.keys(url)[0];
                                    global.distribution.crawler.store.get(urlKey, (e, v) => {
                                        if (v) {
                                            numStored += 1;
                                            if (numStored === expected.length) {
                                                // remove all of the keys from the store
                                                v.forEach((url) => {
                                                    const urlKey = Object.keys(url)[0];
                                                    global.distribution.crawler.store.del(urlKey, (e, v) => {
                                                        if (e) {
                                                            console.error('Error deleting key from store:', e);
                                                        }
                                                        cb(null, v);
                                                    });
                                                });
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
            }
        })
    };

    const sendDataToStore = () => {
        fs.readFile(path.join(__dirname, '../testFiles/urls.txt'), 'utf8', (err, data) => {
            if (err) {
                console.error('Error reading urls.txt:', err);
                done(err);
            } else {
                const urls = data.split('\n');
                let count = 0;
                urls.forEach((url) => {
                    const cleanedKey = url.replace(/[^a-zA-Z0-9]/g, '');
                    distribution.crawler.store.put(url, cleanedKey, (e, v) => {
                        if (e) {
                            console.log(e);
                            console.log(url);
                            console.log(cleanedKey);
                        }
                        count++;
                        if (count === urls.length) {
                            // runMapReduceFlow();
                        }
                    });
                });
            }
        });
    };

    const runMapReduceFlow = () => {
        sendDataToStore(() => {
            doMapReduce((e, v) => {
                const mapReduceResult = v;

                // Check mapReduceResult against expected
                const receivedKeys = mapReduceResult.map((item) => Object.keys(item)[0]);
                const expectedKeys = expected.map((item) => Object.keys(item)[0]);

                // Check if the received keys match the expected keys
                expect(receivedKeys.sort()).toEqual(expectedKeys.sort());

                // Check if each value in the received data is an array
                const receivedData = mapReduceResult.map((item) => {
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

                sendDataToStore(() => {
                    doMapReduce((e, v) => {
                        const mapReduceResult2 = v;
                        console.log("the mapReduceResult2 is: ", mapReduceResult2);

                        // Check mapReduceResult2 against expected2
                        const receivedKeys2 = mapReduceResult2.map((item) => Object.keys(item)[0]);
                        const expectedKeys2 = expected2.map((item) => Object.keys(item)[0]);

                        // Check if the received keys match the expected keys
                        expect(receivedKeys2.sort()).toEqual(expectedKeys2.sort());

                        // Check if each value in the received data is an array
                        const receivedData2 = mapReduceResult2.map((item) => {
                            const key = Object.keys(item)[0];
                            const value = item[key];
                            return { [key]: Array.isArray(value) };
                        });

                        // Check if the received data matches the expected structure
                        expect(receivedData2).toEqual(expect.arrayContaining(
                            expected2.map((item) => {
                                const key = Object.keys(item)[0];
                                return { [key]: true };
                            })
                        ));

                        done();
                    });
                });
            });
        });
    };

    runMapReduceFlow();
});


// i need to figure out the bug where this test isn't working 
// then i probably need to go through the aws and see how that'll work
