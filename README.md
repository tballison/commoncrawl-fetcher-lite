# commoncrawl-fetcher-lite
Simplified version of a common crawl fetcher.
This is yet another attempt to make it easy to extract files from 
[Common Crawl](https://commoncrawl.org/).

> **Warning!!!**
THIS IS STILL ALPHA!  There will be bugs and things will change without warning.

## Goal
Make it easy to extract or refetch a smallish sample (~ couple of million) of complete files from CommonCrawl data.
My primary interest is in binary files, so there's an emphasis on being able to sample
and extract files by mime-type.  If you're building large language models, or if 
you want to process **ALL** of Common Crawl, this project is not for you.

## Background
This section is critical to understanding enough of how CommonCrawl is structured to
make use of this fetcher.

Common Crawl crawls a large portion of the internet each month (roughly 3 billion URLs).

Common Crawl packages the raw files inside [WARC](https://en.wikipedia.org/wiki/Web_ARChive) files, each of which is gzipped and then 
appended to other gzipped WARC files to create compound WARC files stored in AWS's S3 buckets.  
For a single crawl, there are ~90,000 compound WARC files, each ~1 GB in size.  
Uncompressed, a single's month crawl can weigh in at ~400TiB.

>**Note:** Common Crawl truncates files at **1MB**. Researchers who want complete files must refetch
truncated files from the original URLs.

The index files and the compound WARC files are accessible via `S3` (`s3://commoncrawl/`) and HTTPS (`https://data.commoncrawl.org/`).

Users may extract individual gzipped WARC files with an HTTP/S3 range query from the compound WARC files.

To ease access to files, Common Crawl publishes index files, 300 per crawl, totalling 300GB compressed/1TB uncompressed.
Each line of these index files stores information about a fetch of a single URL:

 1. the URL in a format optimized for sorting by host and domain
 2. a timestamp
 3. a JSON object

This is an example of a line:
```
app,web,hurmaninvesterarimli)/89132/31224.html 20230208165905 {"url": "https://hurmaninvesterarimli.web.app/89132/31224.html", "mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": "AQTOHAB777KB7F6E4GRMPNY2IGLGUA2K", "length": "6615", "offset": "325152484", "filename": "crawl-data/CC-MAIN-2023-06/segments/1674764500837.65/warc/CC-MAIN-20230208155417-20230208185417-00224.warc.gz", "charset": "UTF-8", "languages": "slk,eng,spa"}
```

The JSON object above:
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
1. `charset` -- what the http header claimed the charset was (???)
2. `digest` -- the BASE32-encoded SHA1 of the file
3. `filename` -- the name of the compound WARC file as stored in `S3` which contains this individual file's WARC
4. `languages` -- languages as identified by (???)
5. `length` -- length of the compressed WARC file
6. `mime` -- what the http header claimed was the content-type
7. `mime-detected` -- mime type as detected by Apache Tika
8. `offset` -- offset in the compound WARC where this individual WARC file starts
9. `status` -- the http status returned during the fetch
10. `redirect` -- (not included in the example) if a `302`, the redirect URL is stored
11. `truncated` -- (not included in the example) if the file was truncated, this is populated with a cause. The most common cause is `length`.
12. `url` -- the target URL for this file

**Note:** AWS does not allow anonymous access to Common Crawl's S3 bucket -- see [access-the-data](https://commoncrawl.org/access-the-data/).
If you'd prefer to use S3 instead of HTTPS, see [ConfiguringTheFetch](ConfiguringTheFetch.md).

## Steps to run the fetcher
> **Warning!!!**
For each month's crawl, this process downloads ~300GB of index data via HTTPS or S3. If you are running locally
> and you'd prefer to download the index data only once, see [AdvancedScenarios](AdvancedScenarios).

### 1. Specify What is Wanted
At a minimum, the user defines a few parameters in a JSON file ([minimal-config](examples/minimal-config.json), for example).

In the following, we have given a minimal example for extracting all files identified as `mp4` files
from two crawls: `CC-MAIN-2023-06` and `CC-MAIN-2022-49`.  See the [CommonCrawl blog](https://commoncrawl.org/connect/blog/)
for the names of crawls.

The `indices` element contains paths to the index lists for target crawls as described in step 1. The `recordSelector` tells
the fetcher which records to process.  The following requires that the URL have a status of `200`
and that the `mime_detected` value is `video/mp4` or `video/quicktime`.

We have included the optional `dryRun` parameter in the example.  This reports
files that it would have extracted from Common Crawl if the `dryRun` had been set to `false`.

There are many configuration options -- see [ConfiguringTheFetch](ConfiguringTheFetch.md) for more details.

```json
{  
    "dryRun" : true,
    "indices": {
      "paths": [
        "crawl-data/CC-MAIN-2023-06/cc-index.paths.gz",
        "crawl-data/CC-MAIN-2022-49/cc-index.paths.gz"
      ]
    },
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

### 2. Run the Code
Users must have Java (>= 17) installed.  To check your version: `java -version`.

Get the latest released jar from [github](https://github.com/tballison/commoncrawl-fetcher-lite/releases).

The commandline:
`java -jar commoncrawl-fetcher-lite-X.Y.Z.jar minimal-config.json`

This fetcher will extract non-truncated `mp4` files from CommonCrawl's compound WARC files 
and put the `mp4` files in the `docs/` directory.  The fetcher
will write the URLs for the `mp4` files that CommonCrawl truncated
to a file named `logs/urls-truncated.csv` for later re-fetching.

### 3. Refetch Truncated Files
There are many options for this.  The simplest might be [wget](https://www.gnu.org/software/wget/):
`wget -i logs/urls-truncated.csv`

See also options for [curl](https://curl.se/), such as this [article](https://www.baeldung.com/linux/curl-download-urls-listed-in-file).  

The [Nutch project](https://nutch.apache.org/) may be excessive, but it is
extremely scaleable and robust (**it powers Common Crawl!**), and it records the WARC information
for each fetch.

For other scenarios, see [AdvancedScenarios](AdvancedScenarios.md).

## How to Build
For those who want to build the latest, you'll need Java jdk >= 17,
and recent versions of git and maven installed.
1. `git clone https://github.com/tballison/commoncrawl-fetcher-lite`
1. `cd commoncrawl-fetcher-lite`
1. `mvn install`

The jar file will be built in `target/`.

## Design Goals
This is intended to be light-weight.  The current design does not store 
provenance information for the URLs nor for the refetches.  
The goal is simply to select files for extraction and to
extract them or record the URLs for truncated files.

> **Warning!!!**
> AWS throttles download rates, especially for HTTP requests. This code is designed with back-off logic for the default HTTP fetcher so that it will pause 
> if it gets a throttle warning from AWS. While this code is multi-threaded, it is not useful to run more than about 3 threads
> when fetching from HTTP.  When running this code on AWS instances
> pulling from S3, I've run 50 threads without receiving throttling complaints.

## Changes in Common Crawl Indices Over Time
Over time, Common Crawl has added new fields.  If you're pulling from old crawls,
the fields that you need may not exist in the older index files.  For example, `mime-detected`,
was added in the `CC-MAIN-2017-22` crawl. If a value is `null` for a requested field,
there will be logging that warns about a null value for a given field.  If the field
is in a `must_not` clause, that clause will not be considered; if the field is in a `must` or `should`
clause, that record will not be selected.

## Roadmap

Please open bug reports and issues to help prioritize future development.

Some features that could be added are included below.

1. Add more features to the record selector -- handle numeric values (e.g. `int`) and allow for `gt`, `gte` and `range` options
2. Add a refetcher that stores the refetched files with the same policy that the fetcher stores files (same file+directory naming strategy) and potentially writes a table of URLs and fetched digest.
3. Allow a different extracted file naming scheme -- CC's sha1 or any encoding+digest combination?
4. Allow extraction of truncated files.
5. Allow counting of mimes or other features -- not just those selected.
6. Store fetch status and refetch status in an actual database -- against the design goals of this project. LOL...
7. Use parquet files (with or without duckdb and httpfs) instead of the current json-based index files (see this [post](https://avilpage.com/2022/11/common-crawl-laptop-extract-subset.html))
9. ...

## Running the Release

The release is currently triggered by pushing a tag starting with 'v'.

```git tag -a "v1.0.0-alpha1" -m "v1.0.0-alpha1-release" && git push origin v1.0.0-alpha1```

Then navigate to Github and actually make the release.

## Related projects

* [CommonCrawlDocumentDownload](https://github.com/centic9/CommonCrawlDocumentDownload) (my initial inspiration)
* [SimpleCommonCrawlExtractor](https://github.com/tballison/SimpleCommonCrawlExtractor) (my first attempt)
* [commoncrawl-fetcher module in file-observatory](https://github.com/tballison/file-observatory/tree/main/commoncrawl-fetcher) (second attempt)

## For Advanced Users
If you're AWS-savvy, you can more simply select the records for extraction via Athena; 
see this [Common Crawl post](https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/)
on how to run queries against the indices.