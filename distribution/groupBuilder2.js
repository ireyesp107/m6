const express = require('express');
const app = express();
app.use(express.json());
//const distribution = require('../distribution');
const id = require('./util/id')
const fs = require('fs');
const path = require('path');
//console.log("the distribution ", distribution);
const groupsFilePath = path.join(__dirname, 'util', 'groups.txt');
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
    const nodeId = id.getSID(newNode);
    const nodeData = `${nodeId}: ${JSON.stringify(newNode)}\n`;


    fs.appendFile(groupsFilePath, nodeData, (err) => {
        if (err) {
            console.error('Error writing to groups.txt:', err);
            res.status(500).json({ error: 'Internal Server Error' });
        } else {
            res.status(200).json({ message: `Node ${nodeId} added to groups.txt` });
        }
    });


});

const server = app.listen(2999, "172.31.0.115", () => {
    console.log('Express server is running on port 3001');
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