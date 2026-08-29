# EventPulse AI

EventPulse AI turns private event-ticketing exports into practical audience intelligence for venues, promoters, and event organisers.

## Architecture

- **Frontend:** React, Vite, and JavaScript in `frontend/`
- **Backend:** Java 21 and Spring Boot in `backend/`
- **Current data source:** private CSV event exports configured with `EVENTPULSE_DATA_DIRECTORY`
- **Current API:** `GET /api/dashboard`
- **Future:** SQL Server persistence and the broader EventPulse analytics platform

## Development

Start the backend from `backend/` with Maven and the frontend from `frontend/` with npm. The Vite development server proxies `/api` requests to Spring Boot on port 8080.

Private CSV exports, exclusions, and local environment files stay outside version control. Build output and IDE metadata are ignored by `.gitignore`.

The repository contains one active application: the Spring Boot backend and React frontend. Historical implementations are preserved in Git history rather than maintained as additional application roots.
