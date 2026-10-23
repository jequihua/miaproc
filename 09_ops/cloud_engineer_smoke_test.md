# miaproc: cloud staging checks

Use an isolated staging job and test output prefix. Keep the old image,
`cli-r45` tag and production deployment unchanged.

## 1. Pin this image

```text
us-central1-docker.pkg.dev/manglaria-staging/cloud-run-source-deploy/miaproc@sha256:19a0a983993ec00698002e491187db9cfd08924619cd5743e6a0acc4b73e9a20
```

## 2. Configure the job

- Mount representative `flux.parquet` and `biomet.parquet` inputs read-only at `/in`.
- Mount a separate writable test output prefix at `/out`.
- Use writable local scratch at `/scratch`, not the bucket mount.
- Disable automatic retries. Every attempt needs fresh run directory names.
- Allow image pull, input read and output create/read; cleanup also needs output delete permission.
- Set a scratch byte limit below its capacity. Python/R and mount caches need additional memory; Cloud Run in-memory scratch consumes container memory too.

## 3. Run silver, then both gold engines

Run each command in the pinned container using the mounts above. These are Bash
examples; for Cloud Run jobs, set the executable and arguments equivalently.
Use an input window containing enough data for both engines, with `site_id`
identifying the sites. The 256 MiB scratch limit below is a test setting; size it
with the job memory budget for larger inputs. Change `a1` for every new attempt.

```bash
set -e

miaproc eddy run-silver \
  --flux-parquet /in/flux.parquet --biomet-parquet /in/biomet.parquet \
  --group-column site_id --run-dir /out/silver-a1 \
  --output-table /out/silver-a1/silver.parquet \
  --output-run-json /out/silver-a1/silver_run.json \
  --scratch-dir /scratch --scratch-max-bytes 268435456

miaproc eddy validate-run --run-dir /out/silver-a1 --stage silver

for ENGINE in hesseflux-native reddyproc-reference; do
  miaproc eddy run-gold \
    --silver-table /out/silver-a1/silver.parquet \
    --engine "$ENGINE" --repo-root /app --group-column site_id \
    --run-dir "/out/gold-$ENGINE-a1" \
    --output-table "/out/gold-$ENGINE-a1/gold.parquet" \
    --output-diagnostics-json "/out/gold-$ENGINE-a1/gold_diag.json" \
    --output-run-json "/out/gold-$ENGINE-a1/gold_run.json" \
    --scratch-dir /scratch --scratch-max-bytes 268435456
  miaproc eddy validate-run --run-dir "/out/gold-$ENGINE-a1" --stage gold
done
```

Record every exit code; stop this sequence on any nonzero result. All commands
must exit 0 for the normal-path test to pass.

## 4. Check from a separate job/instance

Mount `/out` read-only. Repeat all three `validate-run` commands, then open both
gold Parquet files, for example:

```bash
python -c "import pandas as pd; p='/out/gold-hesseflux-native-a1/gold.parquet'; print(pd.read_parquet(p).groupby('site_id').size())"
```

Repeat for `gold-reddyproc-reference-a1`. Check expected sites, timestamps and row
counts. Record the time until the separate reader can validate the completed
outputs; use bounded backoff if mount caches delay visibility.

## 5. Test failures using disposable test runs only

| Test | Required result |
| --- | --- |
| Repeat an existing run name | Exit 3; existing output unchanged |
| Try creating a disposable marker in the read-only input mount | Write refused |
| Run against output with writes denied | Nonzero exit; no validated output |
| Use a fresh run name with `--scratch-max-bytes 1` | Exit 4; nothing published |
| Interrupt a separate test run | Retain its files; validate before consumption; retry with a fresh name |

Exit 5 or an interrupted/unknown outcome is **not proof of valid output**.
Consume only after `validate-run` exits 0. Never delete a valid run to retry.
Confirm inputs and earlier completed outputs are unchanged after every test.

## 6. Measure and report

Repeat the normal path with the largest expected production window. Record peak
memory, scratch usage and runtime. Have the scientific owner assess representative
results against the established workflow; compare each engine with its own
reference, not with the other engine.

Return the image digest, platform, mount driver/version/options, job CPU/memory,
identity/permissions, commands, exit codes, logs, validation results and resource
measurements. Qualify S3 separately if it will be used. Do not replace the old
deployment until the results are accepted.
