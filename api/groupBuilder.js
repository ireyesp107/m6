const express = require('express');
const app = express();
app.use(express.json());
const id = distribution.util.id;

const crawlerGroup = {};
const urlExtractGroup = {};
const stringMatchGroup = {};
const invertGroup = {};
const reverseLinkGroup = {};
const compactTestGroup = {};
const memTestGroup = {};
const outTestGroup = {};

app.post('/nodes', (req, res) => {
    const newNode = req.body;
    const nodeId = id.getSID(newNode);
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

const server = app.listen(3000, () => {
    console.log('Express server is running on port 3000');
});

module.exports = {
    server,
    crawlerGroup,
    urlExtractGroup,
    stringMatchGroup,
    invertGroup,
    reverseLinkGroup,
    compactTestGroup,
    memTestGroup,
    outTestGroup
};