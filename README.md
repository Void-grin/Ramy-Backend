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

Failover order is:

1. Gemini primary key
2. Gemini secondary key
3. Grok/Groq provider key

Recommended variables:

- `LLM_ONLY_GEMINI=true`
- `GEMINI_API_KEY_PRIMARY=...`
- `GEMINI_MODEL_PRIMARY=gemini-1.5-flash`
- `GEMINI_API_KEY_SECONDARY=...`
- `GEMINI_MODEL_SECONDARY=gemini-1.5-flash`
- `GROK_API_KEY=...`
- `GROK_MODEL=llama-3.3-70b-versatile`
- `GROK_API_URL=https://api.groq.com/openai/v1/chat/completions`

Optional custom fallback after the chain:

- `LLM_API_URL=...`
- `LLM_API_KEY=...`
- `LLM_API_KEY_HEADER=Authorization`
- `LLM_MODEL=...`

When provider is `llm`, the backend forwards comment batches to your LLM API and keeps the same response shape used by the frontend.

## Deploy on Railway

1. Push this backend folder to its own repo (already separated).
2. In Railway, create `New Project` -> `Deploy from GitHub Repo`.
3. Select backend repo and set Root Directory to repo root.
4. Add Environment Variables in Railway:
	- `CLASSIFIER_PROVIDER=llm`
	- `REVIEW_DATA_PATH=../data/augmented/Ramy_data_augmented_target_1500.csv` (or your production path)
	- Gemini/Grok variables listed above
5. Railway will run `npm install` and `npm start` automatically.
6. Copy generated Railway public URL for frontend API base.

Health checks:

- `GET /api/health`
- `GET /api/model/status`

XAI test request:

```bash
curl -X POST http://127.0.0.1:8010/api/model/predict \
	-H "Content-Type: application/json" \
	-d '{"comments":["ramy bnin bzf"],"include_xai":true,"xai_top_k":8}'
```
