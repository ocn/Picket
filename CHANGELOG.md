# Changelog

## [Unreleased]

### Fixed

- Skip missing historical R2Z2 sequence files one at a time instead of stalling
  or resyncing to the current feed head during checkpoint rewind recovery.
