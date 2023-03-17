# commoncrawl-fetcher-lite
Simplified version of a common crawl fetcher.
This is yet another attempt to make it easy to extract files from 
[Common Crawl](https://commoncrawl.org/).

## Goal
Make it easy to extract or refetch a smallish sample (~ couple of million) of complete files from CommonCrawl data.
My primary interest is in binary files, so there's an emphasis on being able to sample
and extract files by mime-type.  If you're building large language models, you'll
want to process the full corpus on Amazon resources.  You will not want to use this project.

## Background
This section is critical to understanding how CommonCrawl is structured and
how this tool works.

Common Crawl crawls a large portion of the internet each month (roughly 3 billion URLs).

The project truncates files at **1MB**. Researchers who want complete files must refetch
truncated files from the original URLs.

Common Crawl packages the raw files inside WARC files, each of which is gzipped and then 
appended to other gzipped WARC files to create very large files stored in AWS's S3 buckets.

The index files and these very large files are accessible via `S3` and HTTPS.

Users may extract individual [WARC](https://en.wikipedia.org/wiki/Web_ARChive) files with an HTTP range query.

To ease access to files, Common Crawl publishes index files, roughly 300GB compressed per crawl.
Each line of these index files contains the following information:

 1. the URL in an archival format
 2. a timestamp
 3. a JSON object

This is an example of a line:
```
app,web,hurmaninvesterarimli)/89132/31224.html 20230208165905 {"url": "https://hurmaninvesterarimli.web.app/89132/31224.html", "mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": "AQTOHAB777KB7F6E4GRMPNY2IGLGUA2K", "length": "6615", "offset": "325152484", "filename": "crawl-data/CC-MAIN-2023-06/segments/1674764500837.65/warc/CC-MAIN-20230208155417-20230208185417-00224.warc.gz", "charset": "UTF-8", "languages": "slk,eng,spa"}
```

The JSON object for some records includes the following:
```
{
  "charset": "UTF-8",
  "digest": "AQTOHAB777KB7F6E4GRMPNY2IGLGUA2K",
  "filename": "crawl-data/CC-MAIN-2023-06/segments/1674764500837.65/warc/CC-MAIN-20230208155417-20230208185417-00224.warc.gz",
  "languages": "slk,eng,spa",
  "length": "6615",
  "mime": "text/html",
  "mime-detected": "text/html",
  "offset": "325152484",
  "status": "200",
  "url": "https://hurmaninvesterarimli.web.app/89132/31224.html"
}
```
1. `charset` -- what the http header claimed the charset was
2. `digest` -- the BASE32-encoded SHA1 of the file
3. `filename` -- is the file as stored in `S3` from which one can extract this file's WARC
4. `languages` -- languages as identified by (???)
5. `length` -- this is the length of the compressed WARC file
6. `mime` -- what the http header claimed was the content-type
7. `mime-detected` -- what the file was identified as by Apache Tika
8. `offset` -- this is the offset in the file defined in `filename` where this files WARC starts
9. `status` -- the http status returned during the fetch
10. `truncated` -- (not included in the example) if the file was truncated, there's a value here. The most common is `length`, but there are other reasons why a file may be truncated
11. `url` -- the target URL for this file

## Steps
### Specify What is Wanted
1. The user collects URLs for crawl indices in a text file -- one file per line. See an [example crawls.txt file](examples/crawls.txt).
2. At a minimum, the user defines a few parameters in a JSON file (`config.json`, for example).

In the following, we have given an example for extracting all files identified as `mp4` files. 

The `indexPathsFile` is the file described in step 1. The `recordSelector` tells
the fetcher which records to process.  The following requires that the URL have a status of `200`
and that the `mime_detected` value is `video/mp4` or `video/quicktime`.

We have included the `dryRun` parameter in the example.  This reports
files that it would have extracted from Common Crawl if the `dryRun` had been set to true.

There are many configuration options -- see [ConfiguringTheFetch](ConfiguringTheFetch.md) for more details.

```
{  
    "dryRun" : true,
    "indexPathsFile": "crawls.txt", 
    "recordSelector": {
        "must": {
            "status": [
                {
                    "match": "200"
                }
            ]
        },
        "should": {
            "mime_detected": [
                {
                    "match": "video/mp4"
                },
                {
                    "match": "video/quicktime"
                }
            ]
        }
    }
}
```

### Run the Code
Users must have Java (> 11) installed.

The commandline:
`java -jar commoncrawl-fetcher-lite-X.Y.Z.jar config.json`

This fetcher will extract non-truncated `mp4` files from CommonCrawl's large WARC files 
and put the `mp4` files in the `docs/` directory.  Further the fetcher
will write a file of URLs for the `mp4` files that but were CommonCrawl truncated
to a file named `urls-for-truncated-files.txt`.

### Refetch Truncated Files
There are many options for this.  The simplest might be:
`wget -i urls-for-truncated-files.txt`

## Design Goals
This is intended to be light-weight.  The current design does not store 
provenance information for the URLs nor for the refetches.  
The goal is simply to select files for extraction and to
extract them or record the URLs for truncated files.

This has been initially designed for users working outside Amazon's environment. 
We will likely add access to `S3` resources in the future.

**NOTE**
AWS throttles the rates at which people may pull data through the public https option.
This code is designed with back-off logic so that it will pause if it gets a throttle warning from AWS.

This code is multithreaded.  However, given AWS's throttling, it is not useful to run more than about 3 threads.

## Roadmap

Some features that could be added.  Please open issues to help prioritize
future development.

1. Allow reads and writes in `S3`.
2. Add more features to the record selector -- handle numeric values (e.g. `int`) and allow for `gt`, `gte` and `range` options
3. Add a refetcher that stores the refetched files with the same policy that the fetcher stores files (same file+directory naming strategy)
4. Allow processing of index files from a local cache.  For exploration, it can be useful to process index files multiple times.  There is no reason to pull 300MB over http for each investigation.
5. Allow a different extracted file naming scheme -- CC's sha1 or any encoding+digest combination?
6. Allow extraction of truncated files.
7. ...