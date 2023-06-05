# Change log

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See [MAINTAINERS.md](./MAINTAINERS.md)
for instructions to keep up to date.

## [1.1.14](https://github.com/streamingfast/firehose-near/releases/tag/v1.1.14)

* Fixed `reader block stats` to print properly time of importing block.

## [1.1.13](https://github.com/streamingfast/firehose-near/releases/tag/v1.1.13)

### Substreams

* Performance: prevent reprocessing jobs when there is only a mapper in production mode and everything is already cached
* Performance: prevent "UpdateStats" from running too often and stalling other operations when running with a high parallel jobs count
* Performance: fixed bug in scheduler ramp-up function sometimes waiting before raising the number of workers

## [1.1.12](https://github.com/streamingfast/firehose-near/releases/tag/v1.1.12)

* 1.1.12 replaces the deleted release 1.1.11, which should not be used

### Operators

> [!IMPORTANT]
> We have had reports of older versions of this software creating corrupted merged-blocks-files (with duplicate or extra out-of-bound blocks)
> This release adds additional validation of merged-blocks to prevent serving duplicate blocks from the firehose or substreams service.
> This may cause service outage if you have produced those blocks or downloaded them from another party who was affected by this bug.

1. Find the affected files by running the following command (can be run multiple times in parallel, over smaller ranges)

```
tools check merged-blocks-batch <merged-blocks-store> <start> <stop>
```

2. If you see any affected range, produce fixed merged-blocks files with the following command, on each range:

```
tools fix-bloated-merged-blocks <merged-blocks-store> <output-store> <start>:<stop>
```

3. Copy the merged-blocks files created in output-store over to the your merged-blocks-store, replacing the corrupted files.

### Added

* Added `tools check merged-blocks-batch` to simplify checking blocks continuity in batched mode, optionally writing results to a store
* Added the command `tools fix-bloated-merged-blocks` to try to fix merged-blocks that contain duplicates and blocks outside of their range.
* Command `tools print one-block and merged-blocks` now supports a new `--output-format` `jsonl` format. Bytes data can now printed as hex or base58 string instead of base64 string.
* Added retry loop for merger when walking one block files. Some use-cases where the bundle reader was sending files too fast and the merger was not waiting to accumulate enough files to start bundling merged files
* Firehose logs now include auth information (userID, keyID, realIP) along with blocks + egress bytes sent.

### Fixed

* Bumped `bstream`: the `filesource` will now refuse to read blocks from a merged-files if they are not ordered or if there are any duplicate.
* The command `tools download-from-firehose` will now fail if it is being served blocks "out of order", to prevent any corrupted merged-blocks from being created.
* The command `tools print merged-blocks` did not print the whole merged-blocks file, the arguments were confusing: now it will parse <start_block> as a uint64.
* The command `tools unmerge-blocks` did not cover the whole given range, now fixed

### Removed

* **Breaking** The `reader-node-log-to-zap` flag has been removed. This was a source of confusion for operators reporting Firehose on <Chain> bugs because the node's logs where merged within normal Firehose on <Chain> logs and it was not super obvious.

  Now, logs from the node will be printed to `stdout` unformatted exactly like presented by the chain. Filtering of such logs must now be delegated to the node's implementation and how it deals depends on the node's binary. Refer to it to determine how you can tweak the logging verbosity emitted by the node.
* Flag substreams-rpc-endpoints removed, this was present by mistake and unused actually.
* Flag substreams-rpc-cache-store-url removed, this was present by mistake and unused actually.
* Flag substreams-rpc-cache-chunk-size removed, this was present by mistake and unused actually.

## 1.1.11

* This version has been deleted because it contains a critical bug: it produced blocks with invalid version number

## [1.1.10](https://github.com/streamingfast/firehose-near/releases/tag/v1.1.10)

* bump firehose-core to `v0.1.12`

## [1.1.9](https://github.com/streamingfast/firehose-near/releases/tag/v1.1.9)

### Changed

* bump firehose-core to `v0.1.10` with regression fix for start block in reversible segment


## [1.1.8](https://github.com/streamingfast/firehose-near/releases/tag/v1.1.8)

### Changed

* bump firehose-core to `v0.1.10` with new metrics `substreams_active_requests` and `substreams_counter`


## [1.1.7](https://github.com/streamingfast/firehose-near/releases/tag/v1.1.7)

* Bumped firehose-core to `0.1.9`
* Go version requirement bumped to `1.21`

## [1.1.6](https://github.com/streamingfast/firehose-near/releases/tag/v1.1.6)

> [!IMPORTANT]
> The Substreams service exposed from this version will send progress messages that cannot be decoded by substreams clients prior to v1.1.12.
> Streaming of the actual data will not be affected. Clients will need to be upgraded to properly decode the new progress messages.

### Changed

* Bumped firehose-core to `0.1.8`
* Bumped substreams to `v1.1.12` to support the new progress message format. Progression now relates to **stages** instead of modules. You can get stage information using the `substreams info` command starting at version `v1.1.12`.
* Migrated to firehose-core
* change block reader-node block encoding from hex to base64

### Removed

*  Removed --substreams-tier1-request-stats and --substreams-tier1-request-stats (substreams request-stats are now always sent to clients)

### Fixed

* More tolerant retry/timeouts on filesource (prevent "Context Deadline Exceeded")

## [1.1.5-rc1](https://github.com/streamingfast/firehose-near/releases/tag/v1.1.5-rc1)

This release candidate is a hotfix for an issue introduced at block v1.1.3 and affecting `production-mode` where the stream will hang and some `map_outputs` will not be produced over some specific ranges of the chains.

## [1.1.4](https://github.com/streamingfast/firehose-near/releases/tag/v1.1.4)

This release bumps substreams to v1.1.10 and firehose-core to v0.1.4

### Fixes

* Fixed: jobs would hang when flags `--substreams-state-bundle-size` and `--substreams-tier1-subrequests-size` had different values. The latter flag has been completely **removed**, subrequests will be bound to the state bundle size.

### Added

* Added support for *continuous authentication* via the grpc auth plugin (allowing cutoff triggered by the auth system).


## [1.1.3](https://github.com/streamingfast/firehose-near/releases/tag/v1.1.3)

This release bumps substreams to v1.1.9 and firehose-core to v0.1.3

### Highlights

#### Substreams Scheduler Improvements for Parallel Processing

The `substreams` scheduler has been improved to reduce the number of required jobs for parallel processing. This affects `backprocessing` (preparing the states of modules up to a "start-block") and `forward processing` (preparing the states and the outputs to speed up streaming in production-mode).

Jobs on `tier2` workers are now divided in "stages", each stage generating the partial states for all the modules that have the same dependencies. A `substreams` that has a single store won't be affected, but one that has 3 top-level stores, which used to run 3 jobs for every segment now only runs a single job per segment to get all the states ready.


#### Substreams State Store Selection

The `substreams` server now accepts `X-Sf-Substreams-Cache-Tag` header to select which Substreams state store URL should be used by the request. When performing a Substreams request, the servers will optionally pick the state store based on the header. This enable consumers to stay on the same cache version when the operators needs to bump the data version (reasons for this could be a bug in Substreams software that caused some cached data to be corrupted on invalid).

To benefit from this, operators that have a version currently in their state store URL should move the version part from `--substreams-state-store-url` to the new flag `--substreams-state-store-default-tag`. For example if today you have in your config:

```yaml
start:
  ...
  flags:
    substreams-state-store-url: /<some>/<path>/v3
```

You should convert to:

```yaml
start:
  ...
  flags:
    substreams-state-store-url: /<some>/<path>
    substreams-state-store-default-tag: v3
```

### Operators Upgrade

The app `substreams-tier1` and `substreams-tier2` should be upgraded concurrently. Some calls will fail while versions are misaligned.

### Backend Changes

* Authentication plugin `trust` can now specify an exclusive list of `allowed` headers (all lowercase), ex: `trust://?allowed=x-sf-user-id,x-sf-api-key-id,x-real-ip,x-sf-substreams-cache-tag`

* The `tier2` app no longer uses the `common-auth-plugin`, `trust` will always be used, so that `tier1` can pass down its headers (ex: `X-Sf-Substreams-Cache-Tag`).

* Fixed some loggers to not render a shortname (so appearing as `<n/a>` in the log).

### CLI Changes

* Added `firenear tools check forks <forked-blocks-store-url> [--min-depth=<depth>]` that reads forked blocks you have and prints resolved longest forks you have seen. The command works for any chain, here a sample output:

    ```log
    ...

    Fork Depth 3
    #45236230 [ea33194e0a9bb1d8 <= 164aa1b9c8a02af0 (on chain)]
    #45236231 [f7d2dc3fbdd0699c <= ea33194e0a9bb1d8]
        #45236232 [ed588cca9b1db391 <= f7d2dc3fbdd0699c]

    Fork Depth 2
    #45236023 [b6b1c68c30b61166 <= 60083a796a079409 (on chain)]
    #45236024 [6d64aec1aece4a43 <= b6b1c68c30b61166]

    ...
    ```

* The `firenear tools` commands and sub-commands have better rendering `--help` by hidden not needed global flags with long description.

## [1.1.2](https://github.com/streamingfast/firehose-near/releases/tag/v1.1.2)

#### Backend Changes

### Highlights

This release brings various renames to fully align with all Firehose <Chain> out there. The repository is now using `firehose-core` which should make easier to follow up with latest Firehose/Substreams feature(s).

This brings in a few breaking changes to align the flags across all chains.

### Breaking Changes

* Removed support for `archive-node` app, if you were using this, please use a standard NEAR Archive node to do the same job.

* Flag `common-block-index-sizes` has been renamed to `common-index-block-sizes`.

* String variable `{sf-data-dir}` which interpolates at runtime to Firehose data directory is now `{data-dir}`. If any of your parameter value has `{sf-data-dir}` in its value, change it to `{data-dir}`.

  > **Note** This is an important change, forgetting to change it will change expected locations of data leading to errors or wrong data.

* The default value for `config-file` changed from `sf.yaml` to `firehose.yaml`. If you didn't had this flag defined and wish to keep the old default, define `config-file: sf.yaml`.

* The default value for `data-dir` changed from `sf-data` to `firehose-data`. If you didn't had this flag defined before, you should either move `sf-data` to `firehose-data` or define `data-dir: sf-data`.

  > **Note** This is an important change, forgetting to change it will change expected locations of data leading to errors or wrong data.

* The flag `verbose` has been renamed to `log-verbosity`.

* The default value for `common-blocks-cache-dir` changed from `{sf-data-dir}/blocks-cache` to `file://{data-dir}/storage/blocks-cache`. If you didn't had this flag defined and you had `common-blocks-cache-enabled: true`, you should define `common-blocks-cache-dir: {data-dir}/blocks-cache`.

* The default value for `common-live-blocks-addr` changed from `:15011` to `:10014`. If you didn't had this flag defined and wish to keep the old default, define `common-live-blocks-addr: 15011` and ensure you also modify `relayer-grpc-listen-addr: :15011` (see next entry for details).

* The default value for `relayer-grpc-listen-addr` changed from `:15011` to `:10014`. If you didn't had this flag defined and wish to keep the old default, define `relayer-grpc-listen-addr: 15011` and ensure you also modify `common-live-blocks-addr: :15011` (see previous entry for details).

* The default value for `relayer-source` changed from `:15010` to `:10010`. If you didn't had this flag defined and wish to keep the old default, define `relayer-source: 15010` and ensure you also modify `reader-node-grpc-listen-addr: :15010` (see next entry for details).

* The default value for `reader-node-grpc-listen-addr` changed from `:15010` to `:10010`. If you didn't had this flag defined and wish to keep the old default, define `reader-node-grpc-listen-addr: :15010` and ensure you also modify `relayer-source: :15010` (see previous entry for details).

* The default value for `reader-node-manager-api-addr` changed from `:15009` to `:10011`. If you didn't had this flag defined and wish to keep the old default, define `reader-node-manager-api-addr: :15010`.

* The `reader-node-arguments` is not populated anymore with default `--home={node-data-dir} <extra-args> run` which means you must now specify those manually. The variables `{data-dir}`, `{node-data-dir}` and `{hostname}` are interpolated respectively to Firehose absolute `data-dir` value, to Firehose absolute `reader-node-data-dir` value and to current hostname. To upgrade, if you had no `reader-node-arguments` defined, you must now define `reader-node-arguments: --home="{node-data-dir}" run`, if you had a `+` in your `reader-node-arguments: +--some-flag`, you must now define it like `reader-node-arguments: --home="{node-data-dir}" --some-flag run`.

  > **Note** This is an important change, forgetting to change it will change expected locations of data leading to errors or wrong data.

* The `reader-node-boot-nodes` flag has been removed entirely, if you have boot nodes to specify, specify them in `reader-node-arguments` using `--boot-nodes=...` instead.

* Removed unused flags `reader-node-merge-and-store-directly`, `reader-node-merge-threshold-block-age` and `reader-node-wait-upload-complete-on-shutdown`.

* The flag `receipt-index-builder-index-size` has been renamed to `index-builder-index-size`.

* The flag `receipt-index-builder-start-block` has been renamed to `index-builder-start-block`.

* The flag `receipt-index-builder-stop-block` has been renamed to `index-builder-stop-block`.

* The default value for `firehose-grpc-listen-addr` changed from `:15042` to `:10015`. If you didn't had this flag defined and wish to keep the old default, define `firehose-grpc-listen-addr: :15010`.

* The default value for `merger-grpc-listen-addr` changed from `:15012` to `:10012`. If you didn't had this flag defined and wish to keep the old default, define `merger-grpc-listen-addr: :15012`.
* Update firehose-core to v0.1.1:
  - added missing `--substreams-tier2-request-stats` request debugging flag
  - added missing firehose rate limiting options flags, `--firehose-rate-limit-bucket-size` and `--firehose-rate-limit-bucket-fill-rate` to manage concurrent connection attempts to Firehose.

## [1.1.1](https://github.com/streamingfast/firehose-near/releases/tag/v1.1.1)

#### Backend Changes

* Fixed Substreams accepted block which was not working properly.

## [1.1.0](https://github.com/streamingfast/firehose-near/releases/tag/v1.1.0)

### Highlights

This release brings various renames to fully align with all Firehose <Chain> out there. The repository is now using `firehose-core` which should make easier to follow up with latest Firehose/Substreams feature(s).

This brings in a few breaking changes to align the flags across all chains.

### Breaking Changes

* Removed support for `archive-node` app, if you were using this, please use a standard NEAR Archive node to do the same job.

* Flag `common-block-index-sizes` has been renamed to `common-index-block-sizes`.

* String variable `{sf-data-dir}` which interpolates at runtime to Firehose data directory is now `{data-dir}`. If any of your parameter value has `{sf-data-dir}` in its value, change it to `{data-dir}`.

  > **Note** This is an important change, forgetting to change it will change expected locations of data leading to errors or wrong data.

* The default value for `config-file` changed from `sf.yaml` to `firehose.yaml`. If you didn't had this flag defined and wish to keep the old default, define `config-file: sf.yaml`.

* The default value for `data-dir` changed from `sf-data` to `firehose-data`. If you didn't had this flag defined before, you should either move `sf-data` to `firehose-data` or define `data-dir: sf-data`.

  > **Note** This is an important change, forgetting to change it will change expected locations of data leading to errors or wrong data.

* The flag `verbose` has been renamed to `log-verbosity`.

* The default value for `common-blocks-cache-dir` changed from `{sf-data-dir}/blocks-cache` to `file://{data-dir}/storage/blocks-cache`. If you didn't had this flag defined and you had `common-blocks-cache-enabled: true`, you should define `common-blocks-cache-dir: {data-dir}/blocks-cache`.

* The default value for `common-live-blocks-addr` changed from `:15011` to `:10014`. If you didn't had this flag defined and wish to keep the old default, define `common-live-blocks-addr: 15011` and ensure you also modify `relayer-grpc-listen-addr: :15011` (see next entry for details).

* The default value for `relayer-grpc-listen-addr` changed from `:15011` to `:10014`. If you didn't had this flag defined and wish to keep the old default, define `relayer-grpc-listen-addr: 15011` and ensure you also modify `common-live-blocks-addr: :15011` (see previous entry for details).

* The default value for `relayer-source` changed from `:15010` to `:10010`. If you didn't had this flag defined and wish to keep the old default, define `relayer-source: 15010` and ensure you also modify `reader-node-grpc-listen-addr: :15010` (see next entry for details).

* The default value for `reader-node-grpc-listen-addr` changed from `:15010` to `:10010`. If you didn't had this flag defined and wish to keep the old default, define `reader-node-grpc-listen-addr: :15010` and ensure you also modify `relayer-source: :15010` (see previous entry for details).

* The default value for `reader-node-manager-api-addr` changed from `:15009` to `:10011`. If you didn't had this flag defined and wish to keep the old default, define `reader-node-manager-api-addr: :15010`.

* The `reader-node-arguments` is not populated anymore with default `--home={node-data-dir} <extra-args> run` which means you must now specify those manually. The variables `{data-dir}`, `{node-data-dir}` and `{hostname}` are interpolated respectively to Firehose absolute `data-dir` value, to Firehose absolute `reader-node-data-dir` value and to current hostname. To upgrade, if you had no `reader-node-arguments` defined, you must now define `reader-node-arguments: --home="{node-data-dir}" run`, if you had a `+` in your `reader-node-arguments: +--some-flag`, you must now define it like `reader-node-arguments: --home="{node-data-dir}" --some-flag run`.

  > **Note** This is an important change, forgetting to change it will change expected locations of data leading to errors or wrong data.

* The `reader-node-boot-nodes` flag has been removed entirely, if you have boot nodes to specify, specify them in `reader-node-arguments` using `--boot-nodes=...` instead.

* Removed unused flags `reader-node-merge-and-store-directly`, `reader-node-merge-threshold-block-age` and `reader-node-wait-upload-complete-on-shutdown`.

* The flag `receipt-index-builder-index-size` has been renamed to `index-builder-index-size`.

* The flag `receipt-index-builder-start-block` has been renamed to `index-builder-start-block`.

* The flag `receipt-index-builder-stop-block` has been renamed to `index-builder-stop-block`.

* The default value for `firehose-grpc-listen-addr` changed from `:15042` to `:10015`. If you didn't had this flag defined and wish to keep the old default, define `firehose-grpc-listen-addr: :15010`.

* The default value for `merger-grpc-listen-addr` changed from `:15012` to `:10012`. If you didn't had this flag defined and wish to keep the old default, define `merger-grpc-listen-addr: :15012`.

## [1.0.6](https://github.com/streamingfast/firehose-near/releases/tag/v1.0.6)

### Highlights

Before this release, the merger would create incorrect merged-blocks bundles if the chain skipped too many block (i.e. skipping a full bundle).

While in some cases, it would not cause any issue while reading over these blocks, it WOULD result in skipped blocks from firehose if the requested start-block was within a problematic range, or even fail to start if given a cursor within that range.

Here are the known affected block ranges showing these problems on NEAR Testnet:
* 102435000-102449000
* 102457500-102458500

### Actions required

* Upgrade your merger deployment to **v1.0.6**
* Run this command to detect ranges with invalid merged-blocks:
`firenear tools check merged-blocks /path/to/merged/blocks   -r 0:123879100 -e |tee /check-all-blocks` (the upper boundary should be adjusted to cover the chain up to the HEAD before you upgraded the merger.
* You will see this kind of output:
```
(...)
❌ invalid block 102448970 in segment 0102448800
❌ invalid block 102458195 in segment 0102457700
❌ invalid block 102458195 in segment 0102457800
(...)
```

* Regroup the affected segments into larger ranges, adding 200 blocks 'before' and 1000 blocks 'after'
* For each of these large ranges (START:END), run the following commands (with **v1.0.6**):

```
# get the one-blocks-files locally
firenear tools unmerge /path/to/your/merged/blocks /tmp/one-blocks START END

# bundle the files into new merged-blocks
firenear start merger --config-file= --common-first-streamable-block=START  --common-merged-blocks-store-url=/tmp/re-merged-blocks --common-one-block-store-url=/tmp/one-blocks --merger-stop-block=END

# copy the new merged files over
cp /tmp/re-merged-blocks/* /path/to/your/merged/blocks/

# clean up your tmp folders
rm /tmp/one-blocks/* /tmp/re-merged-blocks/*
```

## [1.0.5](https://github.com/streamingfast/firehose-near/releases/tag/v1.0.5)

### Changed

* Add s5cmd download to Docker bundle image

## [1.0.4](https://github.com/streamingfast/firehose-near/releases/tag/v1.0.4)

### Changed

* All workflows now use ubuntu-20.04

## [1.0.3](https://github.com/streamingfast/firehose-near/releases/tag/v1.0.3)

### Changed

* More fixes to GitHub workflows

## [1.0.2](https://github.com/streamingfast/firehose-near/releases/tag/v1.0.2)

### Changed

* Update GitHub workflow to use ubuntu-20.04

## [1.0.1](https://github.com/streamingfast/firehose-near/releases/tag/v1.0.1)

### Changed

* **Breaking** Flag `receipt-index-builder-lookup-index-sizes` has been replaced by `common-block-index-sizes`.
    * Migration path is to replace any flag or configuration value named `receipt-index-builder-lookup-index-sizes` by `common-block-index-sizes`.

* **Breaking** Flag `receipt-index-builder-index-store-url` has been replaced by `common-index-store-url`.
    * Migration path is to replace any flag or configuration value named `receipt-index-builder-index-store-url` by `common-index-store-url`.

* **Breaking** Flag `firehose-block-index-url` has been replaced by `common-index-store-url`.
    * Migration path is to replace any flag or configuration value named `firehose-block-index-url` by `common-index-store-url`.

### Added

* Added `firenear tools generate-node-key` command to easily generate a new `node_key.json` file.

## [1.0.0](https://github.com/streamingfast/firehose-near/releases/tag/v1.0.0)

### Added

* Added support for "requester pays" buckets on Google Storage in url, ex: `gs://my-bucket/path?project=my-project-id`

* Added support for Substreams version `v0.2.0` please refer to [release page](https://github.com/streamingfast/substreams/releases/tag/v0.2.0) for further info about Substreams changes.

## [0.3.0](https://github.com/streamingfast/firehose-near/releases/tag/v0.3.0)

* Actual release, no changelog was maintained back then
