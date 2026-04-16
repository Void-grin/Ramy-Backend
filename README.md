# Ramy Node Backend

This backend is fully Node.js/Express and does not depend on Python runtime.

## Run

```bash
cd backend
npm install
npm run dev
```

Default URL: `http://127.0.0.1:8000`

## Key APIs

- `POST /api/model/predict`
- `POST /api/model/predict-file` (CSV/XLSX)
- `GET /api/social/connectors`
- `POST /api/social/connectors`
- `POST /api/social/ingest`
- `GET /api/social/comments`
- `GET /api/social/export.csv`
- `GET /api/social/export.xlsx`
- `GET|POST /api/social/webhook/:platform`

## LLM Integration

Set these values in `backend/.env`:

- `CLASSIFIER_PROVIDER=llm`
- `LLM_API_URL=...`
- `LLM_API_KEY=...`
- `LLM_API_KEY_HEADER=Authorization`
- `LLM_MODEL=...`

When provider is `llm`, the backend forwards comment batches to your LLM API and keeps the same response shape used by the frontend.
