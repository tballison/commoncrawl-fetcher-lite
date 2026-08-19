# Recipe Configs

Ready-to-use configuration files for common file extraction tasks.

**Before using**: Update the `indices.paths` crawl ID to a recent crawl.
Run `ListCrawls` to see available crawls, or use `QuickFetch` to skip
config files entirely.

| Recipe | File types | Config |
|--------|-----------|--------|
| PDFs | PDF | [pdf.json](pdf.json) |
| Office documents | DOCX, XLSX, PPTX, DOC, XLS, PPT + macro-enabled | [office-documents.json](office-documents.json) |
| Spreadsheets | XLSX, XLSM, XLS, ODS | [spreadsheets.json](spreadsheets.json) |
| Images | JPEG, PNG, GIF, WebP, TIFF, BMP, SVG | [images.json](images.json) |
| EPUBs | EPUB | [epub.json](epub.json) |
| Video | MP4, WebM, QuickTime | [video.json](video.json) |
| Outlook messages | MSG | [outlook-msg.json](outlook-msg.json) |
| OneNote | ONE, ONETOC2, ONEPKG | [onenote.json](onenote.json) — columnar variant: [onenote-columnar.json](onenote-columnar.json) |
| RTF | RTF | [rtf.json](rtf.json) |

## Charset-detection corpus building

These recipes target HTML documents that stress-test Tika's encoding detection.
The selector filters on the index's `tld`, `charset`, and `languages` fields —
all derived from raw HTTP / HTML declarations at crawl time, not from any
charset detector, so the resulting samples are not biased by Tika's own picks.

| Recipe | Target | Config |
|--------|--------|--------|
| Cyrillic non-UTF-8 | `.ru/.ua/.by/...` HTML with declared non-UTF-8 charset | [charset-cyrillic-non-utf8.json](charset-cyrillic-non-utf8.json) |
| CJK non-UTF-8 | `.jp/.kr/.cn/.tw/...` HTML with declared non-UTF-8 charset | [charset-cjk-non-utf8.json](charset-cjk-non-utf8.json) |
| Declaration-absent | HTML with no declared charset (pure detection territory) | [charset-declaration-absent.json](charset-declaration-absent.json) |
| Long-tail charsets | Declared KOI8-R/U, IBM866, windows-874/1257, ISO-8859-3/16, etc. | [charset-long-tail.json](charset-long-tail.json) |

## Customizing

1. Copy a recipe to your working directory
2. Update `indices.paths` with the crawl you want (e.g. `CC-MAIN-2025-08`)
3. Update `docs.path` to your desired output directory
4. Adjust `maxFilesExtracted` as needed (-1 for unlimited)
5. Set `dryRun` to `true` if you want to preview counts first

## Or use QuickFetch

```bash
java -jar commoncrawl-fetcher-lite.jar QuickFetch --mime application/pdf --output ~/data/pdfs
java -jar commoncrawl-fetcher-lite.jar QuickFetch --extension xlsx,xlsm --output ~/data/excel
```
