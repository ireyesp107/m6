const express = require('express');
const app = express();
app.use(express.json());

const crawlerGroup = {};
const urlExtractGroup = {};
const stringMatchGroup = {};
const invertGroup = {};
const reverseLinkGroup = {};
const compactTestGroup = {};
const memTestGroup = {};
const outTestGroup = {};

let nodeCounter = 0;

app.post('/nodes', (req, res) => {
    console.log('Request received:', req.body);
    const newNode = req.body;
    // const nodeId = id.getSID(newNode);
    const nodeId = `node_${nodeCounter++}`;
    crawlerGroup[nodeId] = newNode;
    urlExtractGroup[nodeId] = newNode;
    stringMatchGroup[nodeId] = newNode;
    invertGroup[nodeId] = newNode;
    reverseLinkGroup[nodeId] = newNode;
    compactTestGroup[nodeId] = newNode;
    memTestGroup[nodeId] = newNode;
    outTestGroup[nodeId] = newNode;
    res.status(200).json({ message: `Node ${nodeId} added to all groups` });
});

const server = app.listen(3000, "0.0.0.0",() => {
    console.log('Express server is running on port 3000');
});

module.exports = {
    server,
    getCrawlerGroup: () => crawlerGroup,
    getUrlExtractGroup: () => urlExtractGroup,
    getStringMatchGroup: () => stringMatchGroup,
    getInvertGroup: () => invertGroup,
    getReverseLinkGroup: () => reverseLinkGroup,
    getCompactTestGroup: () => compactTestGroup,
    getMemTestGroup: () => memTestGroup,
    getOutTestGroup: () => outTestGroup
};