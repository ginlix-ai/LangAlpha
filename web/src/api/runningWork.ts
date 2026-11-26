import { api } from '@/api/client';

/** What a restart of the server would pause for this user. */
export interface RunningWork {
  turns: number;
  background_tasks: number;
  /** Commands on a computer of the server's own machine, which stop with it. */
  local_commands: number;
}

export async function getRunningWork(): Promise<RunningWork> {
  const { data } = await api.get<RunningWork>('/api/v1/users/me/running-work');
  return data;
}
