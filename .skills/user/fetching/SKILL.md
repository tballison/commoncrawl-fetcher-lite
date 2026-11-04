---
name: fetching
description: >
  Fetching files from Common Crawl with commoncrawl-fetcher-lite: commands, config fields, MIME types, crawl names, monitoring a run. For QueryIndex (columnar index) also load user/columnar-query.
---

# Fetching from Common Crawl

## Config file structure

Config files are JSON. See `examples/` for templates and `examples/recipes/` for common use cases.

Key fields:
- `indices.paths` -- crawl index paths (e.g. `crawl-data/CC-MAIN-2025-51/cc-index.paths.gz`) -- CDX-scan mode (Fetch)
- `columnarIndex.crawlGlob` / `.subset` / `.where` -- QueryIndex mode (Option B) instead of `indices`; `where` is a raw SQL predicate over the Parquet columns, not the `recordSelector` DSL
- `recordSelector.should.mime_detected` -- MIME types to match (Fetch mode only; QueryIndex filters via `columnarIndex.where`)
- `docs.path` -- output directory (auto-created if missing)
- `maxFilesExtracted` -- stop after N files (-1 = unlimited)
- `dryRun` -- if true, scans index but doesn't download files

## Common MIME types for recordSelector

| File type | MIME type |
|-----------|-----------|
| PDF | `application/pdf` |
| DOCX | `application/vnd.openxmlformats-officedocument.wordprocessingml.document` |
| DOCM | `application/vnd.ms-word.document.macroEnabled.12` |
| XLSX | `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` |
| XLSM | `application/vnd.ms-excel.sheet.macroEnabled.12` |
| PPTX | `application/vnd.openxmlformats-officedocument.presentationml.presentation` |
| PPTM | `application/vnd.ms-powerpoint.presentation.macroEnabled.12` |
| DOC | `application/msword` |
| XLS | `application/vnd.ms-excel` |
| PPT | `application/vnd.ms-powerpoint` |
| RTF | `application/rtf` |
| ODT | `application/vnd.oasis.opendocument.text` |
| ODS | `application/vnd.oasis.opendocument.spreadsheet` |
| ODP | `application/vnd.oasis.opendocument.presentation` |
| EPUB | `application/epub+zip` |
| JPEG | `image/jpeg` |
| PNG | `image/png` |
| GIF | `image/gif` |
| WEBP | `image/webp` |
| SVG | `image/svg+xml` |
| TIFF | `image/tiff` |
| BMP | `image/bmp` |
| MP4 | `video/mp4` |
| WEBM | `video/webm` |
| MOV | `video/quicktime` |
| MP3 | `audio/mpeg` |
| WAV | `audio/wav` |
| OGG | `audio/ogg` |
| ZIP | `application/zip` |
| GZIP | `application/gzip` |
| TAR | `application/x-tar` |
| 7Z | `application/x-7z-compressed` |
| RAR | `application/x-rar-compressed` |
| MSG | `application/vnd.ms-outlook` |
| EML | `message/rfc822` |
| XML | `application/xml` |
| JSON | `application/json` |
| CSV | `text/csv` |
| HTML | `text/html` |
| XHTML | `application/xhtml+xml` |

## Crawl naming

Crawls are named like `CC-MAIN-YYYY-WW` (year and week number). The latest as of early 2025 is `CC-MAIN-2025-51`. Use `ListCrawls` to see all available crawls.

## Monitoring progress

- CSV logs live under `logs/<runLabel>/` -- `runLabel` defaults to the last segment of
  `docs.path` (or an explicit `runLabel` config field), so each config gets its own logs
  instead of sharing (and clobbering) one global file. Runs appear to accumulate since
  the appenders use `append="true"`.
- Watch extracted-file count: `wc -l logs/<runLabel>/extracted-urls.csv`
- Watch log output for "finished processing index gz" messages
- Count downloaded files: `find <docs-path> -type f | wc -l`
