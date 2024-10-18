# Advanced Scenarios

## Fetch Index Files Locally
Let's say that you want to run several queries against the index files, and you'd like to store the index files (e.g. `cdx-0000.gz`...)
locally so that you don't have to pull 300GB for each crawl and each time you want to process the index files.

Use this [configuration file](examples/fetch-indices-config.json) as the configuration file to gather the 
`gz` index files from a list of common crawl `cc-index.paths.gz` files.

```json
{
  "indices": {
    "paths": [
      "crawl-data/CC-MAIN-2023-06/cc-index.paths.gz"
    ]
  },
  "docs": {
    "path": "my-indices"
  },
  "numThreads": 2
}
```

Commandline: `java -jar commoncrawl-fetcher-lite-X.Y.Z.jar FetchIndices fetch-indices-config.json`
That will put the index files (~300GB per crawl!) in the local directory `my-indices`, and it will
maintain the original paths so that the other tools may be run against this local cache.

## Extract Selected Files from Local Indices
After downloading the index files locally, you can extract the literal files from the WARCs
using the local index files with [this config](examples/fetch-files-from-local-indices.json).

Commandline: `java -jar commoncrawl-fetcher-lite-X.Y.Z.jar fetch-files-from-local-indices.json`

```json
{
  "numThreads": 2,
  "maxRecords": 100000000,
  "maxFilesExtracted": 100,
  "maxFilesTruncated": 1000,
  "indices": {
    "basePath": "my-indices"
  },
  "indexFetcher" : {
    "basePath": "my-indices"
  },
  "docs": {
    "path": "octets"
  },
  "recordSelector": {
    "should": {
      "mime_detected": [
        {
          "match": "application/octet-stream",
          "sample": 0.8
        }
      ]
    },
    "must": {
      "status": [
        {
          "match": "200"
        }
      ]
    }
  },
  "targetPathPattern": "xx/xx/xxx"
}
```

## Count mimes from local indices

Config file: `count.json`:
```json
{
  "numThreads": 10,
  "indices": {
    "basePath": "my-indices"
  },
  "indexFetcher" : {
    "basePath": "my-indices"
  },
  "recordSelector": {
    "must": {
      "status": [
        {
          "match": "200"
        }
      ]
    }
  }
}
```

Commandline: `java -jar commoncrawl-fetcher-lite-X.Y.Z.jar CountMimes counter.json`