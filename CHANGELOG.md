# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2025-11-11

### Added
- **Enhanced error messages with execution context** - New `NodeExecutionError` exception that wraps all execution failures with comprehensive debugging information
  - Captures the complete execution path showing which nodes were executed
  - Records all inputs passed to the failed node
  - Preserves the original exception for detailed debugging
  - Properly formatted error messages with visual separators that adapt to terminal width
  - Full support for both sync and async execution
  - Picklable for multiprocessing support
- New `error_handling_example.py` demonstrating how to use and handle `NodeExecutionError`

### Changed
- Updated `ExecutionContext` to track execution path during DAG execution
- Modified `run_sync` and `run_async` to wrap exceptions with comprehensive context information
- Enhanced all tests to work with new exception wrapping

### Fixed
- Improved error message formatting with terminal-width-aware separators

## [0.2.1] - 2025-10-15

### Added
- Bumped rustest version for improved testing capabilities

## [0.2.0] - Previous releases

See git history for previous changes.
