# Configuring the fetch

We include a minimal config file in the [README](README.md).

A full config file would include: 

```json
{
  "dryRun": true,
  "fetcher": {
    "throttleSeconds": [ 30, 120, 600, 1800]
  },
  "indices" : {
    "paths": [
      "crawl-data/CC-MAIN-2023-06/cc-index.paths.gz",
      "crawl-data/CC-MAIN-2022-49/cc-index.paths.gz"
    ]
  },
  "docs": {
    "path": "docs"
  },
  "maxFilesExtracted": 10000,
  "maxFilesTruncated": -1,
  "maxRecords": -1,
  "numThreads": 3,
  "recordSelector": {
    "must": {
      "status": [
        {
          "match": "200"
        }
      ]
    },
    "must_not": {
      "status": [
        {
          "match": "300"
        }
      ]
    },
    "should": {
      "mime": [
        {
          "pattern": "(?i)quicktime.*",
          "sample": 0.7
        }
      ],
      "mime_detected": [
        {
          "case_sensitive": false,
          "match": "video/mp4",
          "sample": 0.8
        }
      ]
    }
  },
  "targetPathPattern": "xx/xx/xxx"
}
```
**TODO**: come up with a logical `must` and `must_not` example.  The above is meaningless -- if a record must match 200, it will, obv, not match 300.

## DryRun
If set to `true`, this processes all the records but does not extract the
non-truncated files.  This can be useful for counting files.

## MaxRecords, MaxFilesExtracted, MaxFilesTruncated
If these are set to `-1`, or they are not included, the fetcher will
run against every index record.  A full run for a single month's crawl downloads nearly ~300GB of index files.

If any one of the following is hit, the fetcher stops.

1. `maxRecords` sets a maximum on the records considered for processing, whether
or not the file selector has selected a record for processing.
2. `maxFilesExtracted` sets a maximum on the non-truncated records that are extracted
from the Common Crawl data set.
3. `maxFilesTruncated` sets a maximum on the URLs written to `logs/urls-truncated.csv`.

## Indices
The `indices` element is required. The `paths` element inside the
`indices` element may contain paths to index files (e.g. `.../cdx-00273.gz`) 
or paths to index lists (e.g. `crawl-data/CC-MAIN-2023-06/cc-index.paths.gz`).

An example of paths to specific index files:
```json
{
  "indices" : {
    "paths": [
      "cc-index/collections/CC-MAIN-2023-06/indexes/cdx-00273.gz",
      "cc-index/collections/CC-MAIN-2023-06/indexes/cdx-00274.gz",
      "cc-index/collections/CC-MAIN-2023-06/indexes/cdx-00275.gz",
      "cc-index/collections/CC-MAIN-2023-06/indexes/cdx-00276.gz"
    ]
  }
}
```

An example of paths to lists of index files from specific crawls follows.
If there is no `basePath` or `profile` element, the tool will fetch
these over https from: `https://data.commoncrawl.org/`.
```json
{
  "indices": {
    "paths": [
      "crawl-data/CC-MAIN-2023-06/cc-index.paths.gz",
      "crawl-data/CC-MAIN-2022-49/cc-index.paths.gz"
    ]
  }
}
```

### Index Lists on a Local File System
If the index path lists are on a local file share, they must include a `basePath`
element:
```json
{
  "indices": {
    "basePath" : "C:/Users/mine/cc/index_paths",
    "paths": [
      "crawl-data/CC-MAIN-2023-06/cc-index.paths.gz",
      "crawl-data/CC-MAIN-2022-49/cc-index.paths.gz"
    ]
  }
}
```
### Index Lists on an S3 Bucket
If the index path lists are in s3, they must include a `profile`
element:
```json
{
  "indices": {
    "profile" : "my-aws-cc-profile",
    "paths": [
      "crawl-data/CC-MAIN-2023-06/cc-index.paths.gz",
      "crawl-data/CC-MAIN-2022-49/cc-index.paths.gz"
    ]
  }
}
```

## Fetcher
This is the component that fetches the individual WARC files from
the large composite warc.gz files.

The `fetcher` element is optional.  When the element doesn't exist, the individual
WARCs are fetched over https from: `https://data.commoncrawl.org/` with
default throttle seconds: [30, 120, 600, 1800].  Custom throttle seconds may be specified
for the HTTP fetcher:

```json
{
  "fetcher": {
    "throttleSeconds": [
      30,
      120,
      600,
      1800
    ]
  }
}
```
To fetch the individual WARCs from s3, a fetcher must be defined 
and it must contain a `profile` element:

```json
{
  "fetcher": {
    "profile": "my-s3-cc-profile"
  }
}
```

## Docs
The `docs` element (optional) defines where the extracted files will be written.
If not specified, extracted files will be written to the `docs` subdirectory
of the directory where the tool is run.

To specify a different output directory on a local drive/fileshare:
```json
{
  "docs": {
    "path": "C:/Users/someone/data/docs"
  }
}
```

To specify an S3 bucket:
```json
{
  "docs": {
    "bucket": "my-bucket",
    "profile": "my-profile",
    "region": "us-east-1",
    "prefix": "some-docs"
  }
}
```
Note that `bucket` and `profile` are required. If a region is not specified, `us-east-1` will be used.

## TargetPathPattern
By default, extracted files are stored by the BASE64 encoded SHA-256 of the file in the
`filesDirectory`.  For users extracting hundreds of thousands of files or more, it will
be useful to create subdirectories.

```json
{
    "targetPathPattern":"xx/xxx"
}
```

This would store the `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
file as `e3/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`

If you're extracting millions of files, you may want two levels of subdirectories.
```json
{
    "targetPathPattern":"xx/xx/xxx"
}
```
This would store the file as `e3/b0/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` 

## RecordSelector

The record selector determines whether processing will happen on a URL record.
These are applied to the index records described in the [README](README.md).

### MUST, MUST_NOT, SHOULD
The record selector applies the `must_not` matches first (if the element exists).  If any of those
clauses match a record, the record is ignored.  If the `must` element exists, the record selector requires that all
of its clauses are true for a given record.  If any of the `must` clauses do not apply, the record is
ignored.  Finally, the record selector requires that at least one of the `should` clauses applies to 
a record.

### Fields

1. `mime`
2. `mime_detected`
1. `status`

**TODO:** add the other available fields, url, etc.

### MATCH AND PATTERN Clauses

A `match` is an exact full string match.  It may have a `case_sensitive` flag
that, when set to `false` would allow case_insensitive matches -- default is `true`.

A `pattern` is a regular expression.

Both `match` and `pattern` clauses may include a `sample` element.  This means
that if there is a match or pattern-find on an element, the selector will randomly select
a record that percentage of the time.  For example if you wanted some XHTML (say ~3,000 from a single
crawl), but not nearly all of the 3 billion pages, you could use:

```
...
        "should": {
            "mime_detected": [
                {
                    "match": "application/xhtml+xml",
                    "sample": 0.000001,
                    "case_sensitive": false
                }
            ]
        }
    }
...
```
