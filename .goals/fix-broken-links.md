---
budget:
  maxDuration: 15m
---
# Fix broken links in Markdown documentation

Check Markdown files across the repository, including `README.md`, `AGENTS.md`,
and files under `docs/`, for broken relative file links and broken `#anchor`
fragments. Ignore test fixtures under `testdata/` and do not make external
network requests to check `http://` or `https://` URLs.

Fix a broken link only when the intended target is clear from existing files
and headings. Never fabricate new files, sections, or content to satisfy a
broken link.

If you fix links, commit your changes and call `open_pull_request`. If no
broken links exist, call `finish` with a brief summary.
