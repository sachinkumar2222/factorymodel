# Factory Model HTTP Backend

This directory contains the Node.js backend and dashboard for the HTTP-based Factory Model.

## Components

- `server.js` — HTTP APIs, telemetry handling, device commands, WebSocket updates, and SQLite history.
- `package.json` — Node.js dependencies and scripts.
- `package-lock.json` — locked dependency versions.
- `public/index.html` — Factory Model monitoring and control dashboard.

## Deployment

The backend runs on the Factory Model Mini PC and listens on port 4000. The camera service runs separately on port 8081.

Install dependencies using `npm ci` and start the backend according to the deployment configuration.

## Security

Keep `.env`, device keys, Wi-Fi credentials, runtime database files, and `node_modules` out of Git.
