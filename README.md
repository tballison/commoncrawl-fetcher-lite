# commoncrawl-fetcher-lite

Simplified version of a Common Crawl fetcher.
This is yet another attempt to make it easy to extract files from
[Common Crawl](https://commoncrawl.org/).

> **Warning!!!**
THIS IS STILL ALPHA!  There will be bugs and things will change without warning.

## Goal
Make it easy to extract or refetch a smallish sample (~ couple of million) of complete files from CommonCrawl data.
My primary interest is in binary files, so there's an emphasis on being able to sample
and extract files by mime-type.  If you're building large language models, or if
you want to process **ALL** of Common Crawl, this project is not for you.

## Quick Start
Users must have Java (>= 17) installed.  To check your version: `java -version`.

Get the latest released jar from [GitHub releases](https://github.com/tballison/commoncrawl-fetcher-lite/releases).

```bash
java -jar commoncrawl-fetcher-lite-X.Y.Z.jar --help
java -jar commoncrawl-fetcher-lite-X.Y.Z.jar config.json
```

### QuickFetch (no config file needed)

For simple use cases, skip writing a JSON config:
```bash
# Fetch PDFs from the latest crawl
java -jar commoncrawl-fetcher-lite-X.Y.Z.jar QuickFetch --mime application/pdf --output ~/data/pdfs

# Fetch Excel files
java -jar commoncrawl-fetcher-lite-X.Y.Z.jar QuickFetch --extension xlsx,xlsm --output ~/data/excel

# List available crawls
java -jar commoncrawl-fetcher-lite-X.Y.Z.jar ListCrawls
```

Run `QuickFetch --help` for all options and a list of common MIME types.

See [examples/](examples/) for sample configuration files and [examples/recipes/](examples/recipes/)
for ready-to-use configs for common file types (PDFs, Office docs, images, etc.).

## Documentation

Full documentation is in the [docs/](docs/) directory as AsciiDoc and is also
generated as HTML during the build (`target/docs/`):

* [Overview & Background](docs/index.adoc)
* [Configuring the Fetch](docs/configuration.adoc)
* [Option B: Querying the Columnar Index](docs/columnar-index.adoc) -- faster alternative to
  scanning the CDX index, needs AWS credentials (not Athena, not billed)
* [Advanced Scenarios](docs/advanced.adoc)

To generate HTML docs:
```bash
./mvnw generate-resources
# open target/docs/index.html
```

## How to Build
```bash
git clone https://github.com/tballison/commoncrawl-fetcher-lite
cd commoncrawl-fetcher-lite
./mvnw install
```

The jar file will be built in `target/`.

## Running the Release

The release is currently triggered by pushing a tag starting with 'v'.

```
git tag -a "v1.0.0-alpha1" -m "v1.0.0-alpha1-release" && git push origin v1.0.0-alpha1
```

Then navigate to GitHub and actually make the release.

## Related Projects

* [CommonCrawlDocumentDownload](https://github.com/centic9/CommonCrawlDocumentDownload) (initial inspiration)
* [SimpleCommonCrawlExtractor](https://github.com/tballison/SimpleCommonCrawlExtractor) (first attempt)
* [commoncrawl-fetcher module in file-observatory](https://github.com/tballison/file-observatory/tree/main/commoncrawl-fetcher) (second attempt)
