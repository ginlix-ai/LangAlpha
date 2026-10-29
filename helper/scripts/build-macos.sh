#!/bin/sh
# Builds the helper for both Mac architectures and joins them into the one
# binary that ships, so a single download or app bundle runs on either.
#
# With HELPER_SIGN_IDENTITY set (and HELPER_SIGN_KEYCHAIN, when the identity
# is not in the default search list) it is signed for distribution: hardened
# runtime and a secure timestamp, both of which notarization requires.
# Notarizing is left to the caller, which holds the credentials for it.
set -eu
cd "$(dirname "$0")/.."

# The desktop app's floor, so the helper never refuses a Mac the app runs on.
export MACOSX_DEPLOYMENT_TARGET=12.0
targets="aarch64-apple-darwin x86_64-apple-darwin"
rustup target add $targets >&2
for target in $targets; do
  cargo build --release --locked --target "$target" >&2
done

out=target/universal-apple-darwin/release/langalpha-helper
mkdir -p "$(dirname "$out")"
lipo -create -output "$out" \
  target/aarch64-apple-darwin/release/langalpha-helper \
  target/x86_64-apple-darwin/release/langalpha-helper

if [ -n "${HELPER_SIGN_IDENTITY:-}" ]; then
  codesign --force --options runtime --timestamp \
    --identifier ai.langalpha.helper \
    ${HELPER_SIGN_KEYCHAIN:+--keychain "$HELPER_SIGN_KEYCHAIN"} \
    --sign "$HELPER_SIGN_IDENTITY" "$out" >&2
  codesign --verify --strict --verbose=2 "$out" >&2
fi
echo "$out"
