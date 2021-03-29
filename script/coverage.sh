#!/bin/bash
## Runs code coverage using the tests.

set -e
shopt -s globstar

## Before running, install the needed tools:

# rustup +nightly component add llvm-tools-preview
# cargo +nightly install cargo-binutils
# cargo +nightly install rustfilt

cargo +nightly profdata -- --help

RUSTFLAGS="-Zinstrument-coverage" \
    LLVM_PROFILE_FILE="grebedb-%m.profraw" \
    cargo +nightly test --tests

cargo +nightly profdata -- merge \
    -sparse **/grebedb-*.profraw -o grebedb.profdata

rm **/grebedb-*.profraw

FILES=`RUSTFLAGS="-Zinstrument-coverage" \
    cargo +nightly test --tests --no-run --message-format=json \
        | jq -r "select(.profile.test == true) | .filenames[]" \
        | grep -v dSYM`

FILE_ARGS=""

for file in $FILES; do
    FILE_ARGS+=" --object $file"
done

cargo +nightly cov -- report \
    --use-color --ignore-filename-regex='/.cargo/registry' \
    --instr-profile=grebedb.profdata \
    $FILE_ARGS

cargo +nightly cov -- show \
    --use-color --ignore-filename-regex='/.cargo/registry' \
    --instr-profile=grebedb.profdata \
    $FILE_ARGS \
    --show-instantiations \
    --show-line-counts-or-regions \
    --Xdemangler=rustfilt --format html > grebedb_coverage_report.html

echo "Report placed in grebedb_coverage_report.html"
