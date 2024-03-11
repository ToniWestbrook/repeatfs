# Change Log
## [0.14.0] - 2024-03-10
### Added
- Kafka plugin for streaming intercepted system calls to Kafka server (producer only)

## [0.13.1] - 2023-12-13
### Fixed
- Issue where certain terminal emulators (such as Konsole) would cause an error during the first operation

## [0.13.0] - 2023-12-12
### Added
- Support for systems without procfs support (BSD/MacOS) to run without provenance (ie VDF only mode)

## [0.12.0] - 2023-12-10
### Added
- Snapshot plugin for capturing file contents during read/write operations
- Provenance related callback hooks to plugin system (including HTML rendering)

### Changed
- Reworked plugin routing allowing for default parameters and faster processing

## [0.11.0] - 2023-12-06
### Added
- Plugin system for expanding the functionality of RepeatFS
