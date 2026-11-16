#!/bin/sh
# Builds a universal (arm64 + x86_64) macOS cappy-engine into dist/.
# Needs only the local Go toolchain (env.sh) and the Xcode command-line tools (clang, lipo).
set -eu
cd "$(dirname "$0")/.."
. ./env.sh

export CGO_ENABLED=1
export MACOSX_DEPLOYMENT_TARGET=13.0 # the Go 1.27 runtime itself targets macOS 13
mkdir -p dist tmp.noindex/universal

GOOS=darwin GOARCH=arm64 CC="clang -arch arm64" \
	go build -tags sqlite_fts5 -trimpath -ldflags "-s -w" -o tmp.noindex/universal/cappy-engine-arm64 ./cmd/cappy-engine
GOOS=darwin GOARCH=amd64 CC="clang -arch x86_64" \
	go build -tags sqlite_fts5 -trimpath -ldflags "-s -w" -o tmp.noindex/universal/cappy-engine-x86_64 ./cmd/cappy-engine

lipo -create -output dist/cappy-engine \
	tmp.noindex/universal/cappy-engine-arm64 tmp.noindex/universal/cappy-engine-x86_64
rm -rf tmp.noindex/universal
lipo -archs dist/cappy-engine
ls -l dist/cappy-engine
