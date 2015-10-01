Publishing node-pre-gyp binaries
--------------------------------
Users who are installing `carmen-cache` and not actively doing development usually download prebuilt static binaries at `npm install` time automatically through `node-pre-gyp`.

- Binaries are published by travis to S3 and triggered by including `[publish binary]` in a commit message.
- Binaries are published using the `version` key of `package.json` on the commit with this message.

What this means in practice:

- **Publishing binaries for a release:** make sure `package.json` is updated to the new release version and then commit with `[publish binary]`. This can often be done in the same commit as the release, e.g. `commit -m "5.0.0 [publish binary]"`
- **Publishing for a dev build:** make sure `package.json` uses a dev semver for `version`, e.g. `5.0.0-cache-experiments-v1` and then push a commit with `[publish binary]`.
