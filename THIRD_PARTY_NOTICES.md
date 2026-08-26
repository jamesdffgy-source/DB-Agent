# Third-party notices

DB-Agent is MIT-licensed. The source repository also redistributes the following third-party artifacts under their own licenses.

## Apache ECharts 5.5.1

- File: `runtime/app/frontends/desktop/static/vendor/echarts.min.js`
- Project: <https://echarts.apache.org/>
- License: Apache License 2.0; embedded d3-derived portions are BSD 3-Clause.
- Notice: Copyright 2017-2024 The Apache Software Foundation. This product includes software developed at The Apache Software Foundation (<https://www.apache.org/>).
- License files: `third_party/licenses/APACHE-2.0.txt` and `third_party/licenses/BSD-3-Clause-D3.txt`.

## Python tzdata 2026.3

- File: `runtime/app/frontends/timezone_releases/tzdata-2026.3-iana-2026c.zip`
- Project: <https://github.com/python/tzdata>
- License: Apache License 2.0.
- Copyright (c) 2020, Paul Ganssle (Google); Copyright (c) 2026, Stan Ulbrych.
- License file: `third_party/licenses/APACHE-2.0.txt`.

## Installed Python dependencies

The source distribution does not vendor the Python packages named in `requirements.lock`. They are installed from their publishers by the bootstrap process and retain the license metadata contained in their distributions. Review those package licenses when producing a binary installer.

## Project artwork

The hand-drawn workflow illustration and desktop screenshot under `docs/assets/` are project-owned assets rather than redistributed third-party artifacts. Their handling notes are recorded in `docs/assets/README.md`.
