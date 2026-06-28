# Exchange E2E Engine Setup

The exchange e2e harness owns shared local infra only. It does not start,
stop, or probe `engine_app`.

After `exchange/scripts/e2e-infra.sh up`, run `engine_app` from this repo
against the local Redpanda and MinIO endpoints:

```sh
CEX_ENGINE_BOOTSTRAP_SERVERS=127.0.0.1:19092 \
CEX_ENGINE_CHECKPOINT_STORE=s3 \
CEX_ENGINE_CHECKPOINT_S3_ENDPOINT=http://127.0.0.1:59000 \
CEX_ENGINE_CHECKPOINT_S3_BUCKET=exchange-checkpoints \
CEX_ENGINE_CHECKPOINT_S3_ACCESS_KEY=minioadmin \
CEX_ENGINE_CHECKPOINT_S3_SECRET_KEY=minioadmin \
CEX_ENGINE_CHECKPOINT_S3_REGION=us-east-1 \
CEX_ENGINE_MARKETS_CONFIG=docs/examples/exchange-e2e-markets.conf \
build/engine_app
```

Use the matching port overrides if the exchange infra was started with
`E2E_REDPANDA_PORT` or `E2E_MINIO_PORT`.
