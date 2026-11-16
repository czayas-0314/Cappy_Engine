# Source this file from the engine folder: `source ./env.sh`
# Set GO_ROOT to your Go 1.26+ installation if `go` is not already on PATH.
CAPPY_ENGINE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-${(%):-%x}}")" && pwd)"
export CAPPY_ENGINE_ROOT
if [ -n "${GO_ROOT:-}" ]; then export PATH="$GO_ROOT/bin:$PATH"; elif [ -x "$HOME/.local/go1.27.4/bin/go" ]; then export PATH="$HOME/.local/go1.27.4/bin:$PATH"; fi
export GOPATH="$CAPPY_ENGINE_ROOT/.gopath"
export GOMODCACHE="$CAPPY_ENGINE_ROOT/.gopath/pkg/mod"
export GOCACHE="$CAPPY_ENGINE_ROOT/.gocache"
export GOTOOLCHAIN=local
export GOFLAGS=-mod=mod
