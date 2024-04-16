# M5: Distributed Execution Engine
> Full name: `Josue Cruz`
> Email:  `josue_cruz@brown.edu>`
> Username:  `jcruz14`

## Summary
> Summarize your implementation, including key challenges you encountered

My implementation comprises `1` new software components, totaling `300` added lines of code over the previous implementation. Key challenges included `Conceptual understanding of how to communicate the endings of phases between nodes - TAs helped me gain a better understanding, thought of notify as a concept rather than an actual method I had to write, Race conditions in appending data - wrote a new append method in store/mem, used fs.appendFileSync, Integrating own store service - made a folder for each node during start up, change from async read/writes to synchronous`.

## Correctness & Performance Characterization
> Describe how you characterized the correctness and performance of your implementation

*Correctness*:
To test my mapReduce, I implement the crawler, url extractor, and inverted index workflows. For the crawler workflow, I revised my mapReduce to be able to resolve async map functions so that the crawler can fetch page content. My crawler workflow stores page content in the map phase and I test that each pages content gets stored. For the url extractor, I read test html content from storage in my map function and return all urls. I filter for all unique urls and store them in the reduce phase. For the inverted index, I read test content from storage in my map function where I output an array of all key value pairs, each word in the document to the doc id. In the reduce phase, I filter for all uniq doc ids for each word and return the array of doc ids. I test that the output of the inverted index is what I expected.

*Performance*:
6 tests take 21s to run.
Running in-memory operation for the inverted index workflow took 124ms, compared to 1332ms without in-memory operations. 

Fyi: To run my all.student.test, uncomment lines 47:50 in distribution.js, the autograder doesn't have the libraries installed

## Key Feature
> Which extra features did you implement and how?
I implemented compaction by revising my shuffle phase to run a given compact function after keys are grouped together. The output of the compact function is stored to the appropriate node, where data is appended.
I implemented distributed persistence by storing the result of each pass through the reduce function to the distributed storage, instead of returning the result of the reduce phase to the coordinator. To test whether the results are actually stored correctly in the distributed storage, my coordinator currently gets all keys in the appropriate distributed storage group and returns all key value pairs. I implemented in-memory operation by creating an extra field in the configuration object which tells each phase whether to store intermediate results in the store service or the mem service. The reduce phase also is told to look for the data that the node is responsible for in either the store or mem service.

## Time to Complete
> Roughly, how many hours did this milestone take you to complete?

Hours: `20`

