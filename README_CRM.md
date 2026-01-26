# Denicx AI CRM

A colorful, AI-powered CRM built with PocketBase and vanilla JavaScript/Tailwind CSS.

## Features

- **Pipeline/Kanban view** with color-coded stages
- **Denicx branding** (logo and favicon)
- **AI agent endpoints** for automated lead progression
- **Background jobs** (cron) for autopilot mode
- **PocketBase collections**: leads, accounts, deals, activities
- **Local-first**: runs on `http://127.0.0.1:8090`

## Quick start

```bash
# From this repo root (pocketbase)
go run examples/ai_crm/main.go serve
```

Then open:
- **CRM UI**: http://127.0.0.1:8090/
- **Admin UI**: http://127.0.0.1:8090/_/

## Seeding

The app auto-seeds demo leads on first run. You can also add more via:

```bash
curl -X POST http://127.0.0.1:8090/api/ai-crm/seed \
  -H "Authorization: Bearer <SUPERUSER_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"count": 10}'
```

## Tech

- **Backend**: PocketBase (Go)
- **Frontend**: Vanilla JS + Tailwind CSS
- **Database**: SQLite (managed by PocketBase)
- **AI**: Deterministic logic (LLM integration ready)

## License

See PocketBase license for the framework; this CRM example is MIT.
