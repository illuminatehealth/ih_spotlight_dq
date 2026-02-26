# Third-Party Notices

This project vendors third-party libraries under `distribution/vendor/` to keep the runtime fully offline-capable and usable via `file://` (double-click `distribution/index.html`) with no CDN dependencies.

## Policy

- Only permissively licensed software may be vendored (MIT / Apache-2.0 / ISC).
- Vendored code must be included under `distribution/vendor/<name>/`.
- Each vendored library must include its upstream LICENSE file at `distribution/vendor/<name>/LICENSE` (or `LICENSE.txt`).
- This file must list every vendored component, including version and source location.

---

## Chart.js

- Name: Chart.js
- Version: 4.4.8
- License: MIT
- Source: https://www.npmjs.com/package/chart.js/v/4.4.8
- Local path(s):
  - `distribution/vendor/chart.js/chart.umd.js`
  - `distribution/vendor/chart.js/LICENSE`

Notes:

- Chart.js renders charts to `<canvas>`, which is not screen-reader accessible by default.
- This app provides a visible text legend and summary outside the canvas, plus `aria-label` and canvas fallback text.
