global.nodeConfig = { ip: '127.0.0.1', port: 8070 };
const { Console } = require('console');
const distribution = require('../distribution');
const id = global.distribution.util.id;

//groups added dynamically 
const {
    server,
    getCrawlerGroup,
    getUrlExtractGroup,
    getStringMatchGroup,
    getInvertGroup,
    getReverseLinkGroup,
    getCompactTestGroup,
    getMemTestGroup,
    getOutTestGroup
} = require('../api/groupBuilder');

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

beforeAll((done) => {
    /* Stop the nodes if they are running */

    // Wait for the worker nodes to join the groups
    const checkNodesJoined = () => {
        if (
            Object.keys(getCrawlerGroup()).length === 10 &&
            Object.keys(getUrlExtractGroup()).length === 10 &&
            Object.keys(getStringMatchGroup()).length === 10 &&
            Object.keys(getInvertGroup()).length === 10 &&
            Object.keys(getReverseLinkGroup()).length === 10 &&
            Object.keys(getCompactTestGroup()).length === 10 &&
            Object.keys(getMemTestGroup()).length === 10 &&
            Object.keys(getOutTestGroup()).length === 10
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
    const groups = [crawlerGroup, urlExtractGroup, stringMatchGroup, invertGroup, reverseLinkGroup, compactTestGroup, memTestGroup, outTestGroup];
    let nodeIndex = 0;

    function stopNextNode() {
        if (nodeIndex < groups.length) {
            const group = groups[nodeIndex];
            const nodeIds = Object.keys(group);

            if (nodeIds.length > 0) {
                remote.node = group[nodeIds[0]];
                distribution.local.comm.send([], remote, (e, v) => {
                    delete group[nodeIds[0]];
                    stopNextNode();
                });
            } else {
                nodeIndex++;
                stopNextNode();
            }
        } else {
            localServer.close();
            done();
        }
    }

    stopNextNode();
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

test('all.mr:crawler-homepage-urltxt-multiple rounds', (done) => {
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

                //const lowerCaseBody = global.distribution.convert(body).toLowerCase();
                //console.log(lowerCaseBody)
                // Extract URLs from the content
                const extractedUrls = extractLinks(body, value).filter(url => url !== undefined);

                let out = {};
                out['Brown'] = extractedUrls;
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
            if (iteration > 4 || data === '') {
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

                            distribution.crawler.mr.exec({ keys: filteredKeys, map: m1, reduce: r1, iterations: 1 }, (e, v) => {
                                try {
                                    console.log(v)

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

                                        }
                                    });
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
        };

        doMapReduce();

    });
});
