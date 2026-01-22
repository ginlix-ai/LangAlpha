# MSW (Mock Service Worker) Setup Guide

## Overview

This project uses MSW to mock backend API calls when the backend is not fully available. This allows frontend development and testing to continue independently.

## Initial Setup

1. **Generate the Service Worker file** (one-time setup):
```bash
npx msw init public/ --save
```

This creates `public/mockServiceWorker.js` which is required for MSW to work in the browser.

## Configuration

### Enable/Disable MSW

Control whether to use mock server or real backend using environment variables:

1. **Create a `.env` file** in the project root (if it doesn't exist):
```bash
# Use MSW mock server
VITE_USE_MSW=true

# OR use real backend (default)
VITE_USE_MSW=false
```

2. **Restart the dev server** after changing the `.env` file:
```bash
npm run dev
```

## How It Works

- **When `VITE_USE_MSW=true`**: MSW intercepts all fetch requests and returns mock responses
- **When `VITE_USE_MSW=false` or unset**: Requests go to the real backend server

## Adding Mock Handlers

Mock handlers are located in:
- `src/pages/Dashboard/mocks/handlers.js` - Dashboard-specific mocks

To add more mock handlers:

1. Add handlers to the appropriate `handlers.js` file
2. Import and add them to `src/mocks/browser.js`:

```javascript
import { handlers as dashboardHandlers } from '../pages/Dashboard/mocks/handlers';
import { handlers as otherHandlers } from '../pages/Other/mocks/handlers';

export const worker = setupWorker(...dashboardHandlers, ...otherHandlers);
```

## Example Handler

```javascript
import { http, HttpResponse } from 'msw';

export const handlers = [
  http.get('http://localhost:8080/hello', () => {
    return HttpResponse.text('Hello from Mock Service Worker!');
  }),
];
```

## Troubleshooting

- If MSW doesn't work, make sure `public/mockServiceWorker.js` exists
- Check the browser console for MSW initialization messages
- Verify your `.env` file has `VITE_USE_MSW=true` set correctly
- Restart the dev server after changing `.env` files
