import { http, HttpResponse } from 'msw';

export const handlers = [
  http.get('http://localhost:8080/hello', () => {
    return HttpResponse.text('Hello from Mock Service Worker!');
  }),
];
