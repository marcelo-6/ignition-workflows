<!-- markdownlint-disable -->
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.5] - 2026-02-20

### <!-- 1 -->Bug Fixes
- Introduce in-memory flush dead-letter handling to stop infinite requeue loops on insert failures  by @marcelo-6 in [#34](https://github.com/marcelo-6/ignition-workflows/pull/34) ([d13eff9](https://github.com/marcelo-6/ignition-workflows/commit/d13eff98d1987c796e8b8bfe772fc2c42aef487a))

- Fixed dedup index to block enqueing of active workflows only  by @marcelo-6 ([2e12d87](https://github.com/marcelo-6/ignition-workflows/commit/2e12d87bcca12052d06c52f2f379f0857b53a1d3))

- Fixed dispatch logic to prioritize lower values (matches DBOS)  by @marcelo-6 ([0e18b7b](https://github.com/marcelo-6/ignition-workflows/commit/0e18b7b24e7e2323f3a2bca7154ef934a07aa274))

- Fixed startWithTemplate return and related views  by @marcelo-6 ([17f9b98](https://github.com/marcelo-6/ignition-workflows/commit/17f9b9879295f5ef3e5b53ecdd91dc5453651758))

- Removed legacy param set from db schema  by @marcelo-6 ([4b37796](https://github.com/marcelo-6/ignition-workflows/commit/4b377968397631897e39ca8f5e74975eff7b2122))

- Fixed listTemplates latest-only malformed query  by @marcelo-6 ([934063c](https://github.com/marcelo-6/ignition-workflows/commit/934063c4fe8a52bc6fedff66331a39a65552baa1))


### <!-- 3 -->Documentation
- Added total downloads badge  by @marcelo-6 ([8d5ca17](https://github.com/marcelo-6/ignition-workflows/commit/8d5ca17ba3c0f2249279cbda5fcd2077aa25a982))

- Removed some badges  by @marcelo-6 ([b4a5da8](https://github.com/marcelo-6/ignition-workflows/commit/b4a5da8150663f9db8c01e4ea8c734e2890ff89f))


### <!-- 6 -->Testing
- Added test for queue priority/dedupe ID  by @marcelo-6 ([c1c1344](https://github.com/marcelo-6/ignition-workflows/commit/c1c13443067f843e6d536129dbdde1744dfcf269))

- Added test for startWithTemplate to confirm return format  by @marcelo-6 ([d29db29](https://github.com/marcelo-6/ignition-workflows/commit/d29db2995a378eaa6dc3aa62b805220e14ccc822))

- Added params test suite  by @marcelo-6 ([3a29954](https://github.com/marcelo-6/ignition-workflows/commit/3a299544150245db450eb59f872cefb83299b328))


### <!-- 7 -->Miscellaneous Tasks
- Removed legacy timer script  by @marcelo-6 ([83aa71f](https://github.com/marcelo-6/ignition-workflows/commit/83aa71f490918c0af267caf566d161cffeb95c4c))

- Added examples of templates and enums to be loaded in db  by @marcelo-6 ([1bc2869](https://github.com/marcelo-6/ignition-workflows/commit/1bc2869201030f7def520cc77377e1abe832664a))


## [0.3.4] - 2026-02-18

### <!-- 7 -->Miscellaneous Tasks
- Updated codeowners  by @marcelo-6 in [#27](https://github.com/marcelo-6/ignition-workflows/pull/27) ([cc8aee9](https://github.com/marcelo-6/ignition-workflows/commit/cc8aee95ca59fb908cac7c4143559cdf099c8bdc))

- Added deploy key to cd pipeline  by @marcelo-6 ([229efa3](https://github.com/marcelo-6/ignition-workflows/commit/229efa30979e3bdf958d79c31dea0b375caeeaac))

- Updated git cliff changelog template  by @marcelo-6 ([03824e2](https://github.com/marcelo-6/ignition-workflows/commit/03824e22af36c426a7fbfe13849b3d62f562b10c))

- Updated ci/cd  by @marcelo-6 ([e185596](https://github.com/marcelo-6/ignition-workflows/commit/e1855962a145a2f58c601cafbf3977d10609acbd))

- Added commit id to changelog template  by @marcelo-6 ([16f43d1](https://github.com/marcelo-6/ignition-workflows/commit/16f43d1d0c69406959f615f012be2e03d43546d9))

- Updated ci/cd pipelines  by @marcelo-6 ([f538a44](https://github.com/marcelo-6/ignition-workflows/commit/f538a444d9e304cef8def17d73cfce1a9b6c18f9))

- Added pre-commit hooks for linting like the CI pipeline  by @marcelo-6 ([1b89413](https://github.com/marcelo-6/ignition-workflows/commit/1b89413e1d1e771b3f1a2a8d147f780234555963))

- Added ignition lint  by @marcelo-6 ([418290c](https://github.com/marcelo-6/ignition-workflows/commit/418290c94de88e3c2d4da44bce7fb46cfbac1167))

- Markdown lint  by @marcelo-6 ([16de590](https://github.com/marcelo-6/ignition-workflows/commit/16de5909a5d7bdc4ff3ca9d7fce9d96ad19e7265))

- Yml lint  by @marcelo-6 ([c03f76f](https://github.com/marcelo-6/ignition-workflows/commit/c03f76f0f586ee3f13c7a57817550b6e7538feba))

- Clearner pipelines  by @marcelo-6 ([b6c6d78](https://github.com/marcelo-6/ignition-workflows/commit/b6c6d78550dcd87a6b40e4ad8372693b12b80df7))


## [0.3.3] - 2026-02-17

### <!-- 2 -->Refactor
- Align exchange.workflows.engine.db with Exchange code standard  by @marcelo-6 in [#20](https://github.com/marcelo-6/ignition-workflows/pull/20) ([5ea9d03](https://github.com/marcelo-6/ignition-workflows/commit/5ea9d036480d2a3eb30c566bb418195637bdce96))


### <!-- 3 -->Documentation
- Added better documentation  by @marcelo-6 ([4b6f11d](https://github.com/marcelo-6/ignition-workflows/commit/4b6f11d718039ca1c58d1a18fcd8205694710413))

- Added repository link to project documentation  by @marcelo-6 ([98075c3](https://github.com/marcelo-6/ignition-workflows/commit/98075c3941f2a6fd391211e313295d664c5404e0))

- Added project documentation  by @marcelo-6 ([60192d6](https://github.com/marcelo-6/ignition-workflows/commit/60192d6daf5f4ac2061528a536f8c68cd6f610f6))


### <!-- 7 -->Miscellaneous Tasks
- Clean up old test suite  by @marcelo-6 ([882fc0f](https://github.com/marcelo-6/ignition-workflows/commit/882fc0fa030e53548ca67fbd1b8a7abf4f2dc2e7))

- Clearned up unused project files  by @marcelo-6 ([fa576f5](https://github.com/marcelo-6/ignition-workflows/commit/fa576f52577124026f6bd8273518bddfad2fa579))


### Other
- Added modl examples to dockerfile and env  by @marcelo-6 ([ef01ad6](https://github.com/marcelo-6/ignition-workflows/commit/ef01ad67a866ca335e401cd8603740eb0a085684))


## [0.3.1] - 2026-02-16

### Other
- add Zensical docs (#7)  by @marcelo-6 in [#7](https://github.com/marcelo-6/ignition-workflows/pull/7) ([6a3f8b4](https://github.com/marcelo-6/ignition-workflows/commit/6a3f8b4378fcb15d63a0c7a143f6f30612c21c60))


## [0.3.0] - 2026-02-16

### <!-- 7 -->Miscellaneous Tasks
- Updated changelog generation (#6)  by @marcelo-6 in [#6](https://github.com/marcelo-6/ignition-workflows/pull/6) ([816240e](https://github.com/marcelo-6/ignition-workflows/commit/816240ec348ebc1fbfd925a5d262fd05eddb10ae))


## [0.2.0] - 2026-02-16

### <!-- 7 -->Miscellaneous Tasks
- Update changelog.md generation (#5)  by @marcelo-6 in [#5](https://github.com/marcelo-6/ignition-workflows/pull/5) ([1842a7a](https://github.com/marcelo-6/ignition-workflows/commit/1842a7a427fd1c502b6caa00b44545158d611f46))


## [0.1.0] - 2026-02-16

### <!-- 7 -->Miscellaneous Tasks
- Add release-check and make release workflow publish only (#4)  by @marcelo-6 in [#4](https://github.com/marcelo-6/ignition-workflows/pull/4) ([62153ea](https://github.com/marcelo-6/ignition-workflows/commit/62153ea6b0507488380c72869a0a0e1a947bc88d))

- Fixed changelog generation workflow failure (#3)  by @marcelo-6 in [#3](https://github.com/marcelo-6/ignition-workflows/pull/3) ([74b6423](https://github.com/marcelo-6/ignition-workflows/commit/74b6423d706d02765bd578976fbe10a7c0e3be1b))

- Added changelog generation as part of github action release workflow (#2)  by @marcelo-6 in [#2](https://github.com/marcelo-6/ignition-workflows/pull/2) ([d05378d](https://github.com/marcelo-6/ignition-workflows/commit/d05378d57295fdf93a0722bd78b5419ef17f167a))


## [0.0.1] - 2026-02-16

### <!-- 7 -->Miscellaneous Tasks
- Added release github actions (#1)  by @marcelo-6 in [#1](https://github.com/marcelo-6/ignition-workflows/pull/1) ([1924645](https://github.com/marcelo-6/ignition-workflows/commit/192464567f897e42b034f60cf129b0e8596eea5c))


### Other
- Initial public release  by @marcelo-6 ([f147646](https://github.com/marcelo-6/ignition-workflows/commit/f1476463daa6762394a84da8f5046391db251cad))


### New Contributors
* @marcelo-6 made their first contribution in [#1](https://github.com/marcelo-6/ignition-workflows/pull/1)

[0.3.4]: https://github.com/marcelo-6/ignition-workflows/compare/v0.3.3...v0.3.4
[0.3.3]: https://github.com/marcelo-6/ignition-workflows/compare/v0.3.1...v0.3.3
[0.3.1]: https://github.com/marcelo-6/ignition-workflows/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/marcelo-6/ignition-workflows/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/marcelo-6/ignition-workflows/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/marcelo-6/ignition-workflows/compare/v0.0.1...v0.1.0

<!-- generated by git-cliff -->
