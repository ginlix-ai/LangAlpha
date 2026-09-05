const dtf = new Intl.DateTimeFormat('en-US', {
  month: 'short',
  day: 'numeric',
  year: 'numeric',
  hour: 'numeric',
  minute: '2-digit',
});

export function formatDateTime(date: string | Date | null | undefined): string {
  if (!date) return '\u2014';
  const d = typeof date === 'string' ? new Date(date) : date;
  return dtf.format(d);
}

export function formatDuration(startDate: string | Date | null | undefined, endDate: string | Date | null | undefined): string {
  if (!startDate || !endDate) return '\u2014';
  const start = typeof startDate === 'string' ? new Date(startDate) : startDate;
  const end = typeof endDate === 'string' ? new Date(endDate) : endDate;
  const ms = end.getTime() - start.getTime();

  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${Math.round(ms / 1000)}s`;
  const mins = Math.floor(ms / 60000);
  const secs = Math.round((ms % 60000) / 1000);
  return secs > 0 ? `${mins}m ${secs}s` : `${mins}m`;
}
