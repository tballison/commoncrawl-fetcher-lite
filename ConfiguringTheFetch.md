# Configuring the fetch

We include a minimal config file in the [README](README.md).

A full config file would include: 

```json
{
  "dryRun": true,
  "filesDirectory": "docs",
  "indexPathsFile": "crawls.txt",
  "maxFilesExtracted": 10000,
  "maxFilesTruncated": -1,
  "maxRecords": -1,
  "numThreads": 2,
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
  "targetPathPattern": "xx/xx/xxx",
  "truncatedUrlsFile": "urls-for-truncated-files.txt"
}
```

## MaxRecords, MaxFilesExtracted, MaxFilesTruncated
If these are all set to `-1`, or they are not included, the fetcher will
run against every index record.

If any one of the following is hit, the fetcher stops.

1. `maxRecords` sets a maximum on the records considered for processing, whether
or not the file selector has selected a record for processing.
2. `maxFilesExtracted` sets a maximum on the non-truncated records that are extracted
from the Common Crawl data set.
3. `maxFilesTruncated` sets a maximum on the URLs written to `urls-for-truncated-files.txt`.

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
clauses match a record, the record is passed over.  If the `must` element exists, the record selector requires that all
of its clauses are true for a given record.  If any of the `must` clauses do not apply, the record is
passed over.  Finally, the record selector requires that at least one of the `should` clauses applies to 
a record.

### Fields

1. `mime`
2. `mime_detected`
1. `status`

### MATCH AND PATTERN Clauses

A `match` is an exact full string match.  It may have a `case_sensitive` flag
that would allow for matches irrespective of case.

A `pattern` is a regular expression.

Both `match` and `pattern` clauses may include a `sample` element.  This means
that if there is a match or pattern-find on an element, the selector will randomly select
a record that percentage of the time.  For example if you wanted some XHTML (say 3,000 from a single
crawl), but not 3 billion pages, you could use:

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
