import type { MentionedFile, SlashCommand } from '@/components/ui/chat-input.types';

/** A file of a draft, as saved: enough to rebuild the File from its bytes and
 *  to name the server's copy, if one was ready. */
export interface DraftFileMeta {
  id: string;
  name: string;
  /** The composer tile's type. */
  type: string;
  /** The File's own type. */
  mime: string;
  size: number;
  lastModified: number;
  attachmentId?: string;
  readyAt?: number;
}

export interface DraftContent {
  text: string;
  mentions: MentionedFile[];
  commands: SlashCommand[];
  files: DraftFileMeta[];
}
