global.nodeConfig = { ip: '127.0.0.1', port: 7591 };
const distribution = require('../distribution');
const id = distribution.util.id;

const groupsTemplate = require('../distribution/all/groups');
const fs = require('fs');
const path = require('path');
global.fetch = require('node-fetch');

const crawlerGroup = {};
const indexGroup = {};
const indexDataGroup = {};
const invertIndexGroup = {};


const compactTestGroup = {};
const memTestGroup = {};
const outTestGroup = {};
let uniqueKey = 0;
let iteration = 0;
jest.setTimeout(6000000)

let localServer = null;
const n1 = { ip: '127.0.0.1', port: 7932 };
const n2 = { ip: '127.0.0.1', port: 7933 };
const n3 = { ip: '127.0.0.1', port: 7934 };
const n4 = { ip: '127.0.0.1', port: 7935 };
const n5 = { ip: '127.0.0.1', port: 7936 };
const n6 = { ip: '127.0.0.1', port: 7937 };
const n7 = { ip: '127.0.0.1', port: 7938 };
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
                    const indexConfig = { gid: 'index' };
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
test('basic crawler', (done) => {
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
            if (cntr === dataset.length) {
                doMapReduce();
            }
        });
    });
});

test('two seperate mr', (done) => {
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
                const stringBody = body
                const extractedUrls = extractLinks(body, value).filter(url => url !== undefined);
                global.distribution.index.store.put([stringBody, value], key, (e, v) => { });

                let out = {};
                out['Brown'] = extractedUrls;
                return out;
            }
            catch (e) {
                console.error('Error fetching data for ' + value, e);

                return {}
            }
        };

        let r1 = (key, values) => {

            let out = {};
            out[key] = values.flat();
            return out;
        };


        let mIndex = async (key, value) => {
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
                    const firstTwoLetters = item.word.slice(0, 2);

                    if (!grouped[firstTwoLetters]) {
                        grouped[firstTwoLetters] = [];
                    }

                    item['url'] = url;

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

            let cntr = 0
            let toDelete = []
            fs.appendFileSync(path.join(__dirname, '../testFiles/seenUrls.txt'), data + '\n')

            urlsDataset.forEach((o) => {
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

                            distribution.crawler.mr.exec({ keys: filteredKeys, map: m1, reduce: r1, iterations: 1, memory: true }, (e, v) => {
                                try {
                                    console.log(v)
                                    const seenUrlData = fs.readFileSync(path.join(__dirname, '../testFiles/seenUrls.txt'), { encoding: 'utf8' });
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
                                            distribution.index.mr.exec({ keys: filteredKeys, map: mIndex, reduce: rIndex, memory: true }, (_, v) => {
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

                                                                        const uniqueArray = [...uniqueMap.values()];

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
                                                                                                        distribution.crawler.comm.send([{ key: 'tempResults', gid: 'index' }], remote, (e, v) => {
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

test('one mr, full crawler and index', (done) => {
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
                
                const extractedUrls = extractLinks(body, value).filter(url => url !== undefined);
                if (extractedUrls.length > 0) {
                    global.distribution.indexData.store.append(extractedUrls, 'tempUrls', (e, v) => { })
                }

                function makeBigrams(words) {
                    const bigrams = [];
                    for (let i = 0; i < words.length - 1; i++) {
                        bigrams.push([words[i], words[i + 1]]);
                    }
                    return bigrams;
                }

                function makeTrigrams(words) {
                    const trigrams = [];
                    for (let i = 0; i < words.length - 2; i++) {
                        trigrams.push([words[i], words[i + 1], words[i + 2]]);
                    }
                    return trigrams;
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
                            counts[word] += 1;
                        } else {
                            counts[word] = 1;
                        }
                    });

                    const result = Object.keys(counts).map(key => ({
                        count: counts[key],
                        word: key
                    }));

                    result.sort((a, b) => b.count - a.count);

                    return result.slice(0, 100);
                }

                function groupWordsByFirstLetter(wordCounts, url) {
                    const grouped = {};

                    wordCounts.forEach(item => {
                        const firstTwoLetters = item.word.slice(0, 2);

                        if (!grouped[firstTwoLetters]) {
                            grouped[firstTwoLetters] = [];
                        }

                        item['url'] = url;
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
                let bigramCounts = countWords(makeBigrams(filteredWords))
                let trigramCounts = countWords(makeTrigrams(filteredWords))
                let allCounts = wordCounts.concat(bigramCounts, trigramCounts)
                let groupedWords = groupWordsByFirstLetter(allCounts, currURL)
                let out = groupedWords;

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
                    if (wordItem && wordItem['word']) {
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

        const doMapReduce = () => {
            let data = checkFileEmpty().trim()
            if (iteration > 3 || data === '') {
                done();
                return;
            }
            iteration++;

            let urlsDataset = ''
            urlsDataset = data.split('\n');

            let cntr = 0
            let toDelete = []
            urlsDataset.forEach((o) => {
                uniqueKey += 1
                toDelete.push(uniqueKey)
                distribution.crawler.store.put(o, uniqueKey.toString(), (e, v) => {
                    if (e) {
                        done(e);
                    }

                    cntr++;
                    if (cntr === urlsDataset.length) {
                        distribution.crawler.store.get(null, (e, v) => {
                            let filteredKeys = v.filter(element => !isNaN(element));
                            distribution.crawler.mr.exec({ keys: filteredKeys, map: m1, reduce: r1, iterations: 1, memory: true }, (e, kvPairs) => {
                                try {
                                    // Save the output to urls.txt
                                    distribution.indexData.store.get('tempUrls', (e, v) => {
                                        const urlsToSave = v.join('\n')
                                        distribution.indexData.store.del('tempUrls', (e, v) => {
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

                                                                        let uniqueArray = Array.from(uniqueMap.values());

                                                                        uniqueArray = uniqueArray.sort((a, b) => b.count - a.count).slice(0, 10);
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
                                    })
                                } catch (e) {
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