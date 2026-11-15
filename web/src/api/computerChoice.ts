/**
 * Where the user chose to work when they had no computer: their own machine
 * or the cloud. The desktop app's first run asks once; null is never asked.
 * It decides only while the user has no primary computer: `own_machine`
 * keeps a cloud computer from being built for them.
 */
import { api } from '@/api/client';

export type ComputerChoice = 'own_machine' | 'cloud';

export interface ComputerChoiceResponse {
  choice: ComputerChoice | null;
}

export async function getComputerChoice(): Promise<ComputerChoiceResponse> {
  const { data } = await api.get<ComputerChoiceResponse>('/api/v1/users/me/computer-choice');
  return data;
}

export async function setComputerChoice(choice: ComputerChoice): Promise<ComputerChoiceResponse> {
  const { data } = await api.put<ComputerChoiceResponse>('/api/v1/users/me/computer-choice', { choice });
  return data;
}
