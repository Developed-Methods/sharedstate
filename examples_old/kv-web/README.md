# Sharedstate KV Web Example

Build the frontend and run the Rust example server:

```bash
cd examples/kv-web
npm install
npm run build
cd ../..
cargo run --example kv_web -- --bind 127.0.0.1:8080
```

For frontend development, run the API server and Vite separately:

```bash
cargo run --example kv_web -- --bind 127.0.0.1:8080
```

```bash
cd examples/kv-web
npm run dev
```

The Vite dev server proxies `/api` to `http://127.0.0.1:8080`.
