import { useEffect, type RefObject } from 'react';
import type { ChatInputHandle } from '@/components/ui/chat-input';
import type { ComposerDraft } from '@/components/ui/chat-input.types';

/** What the chat hands back to its composer, each with its clear. */
interface ReturnedToComposer {
  returnedSteering: string | null;
  clearReturnedSteering: () => void;
  returnedEdit: string | null;
  clearReturnedEdit: () => void;
  returnedDraft: ComposerDraft | null;
  clearReturnedDraft: () => void;
}

/**
 * Puts back into the composer what the chat returns: steering the agent
 * finished without reading and edit text whose message is gone, beside
 * whatever was typed since, and a send that failed at a file's foot, whole,
 * its files and pills too, leaving focus where it is, since the person may
 * have moved on meanwhile.
 */
export function useComposerGiveBack(input: RefObject<ChatInputHandle | null>, chat: ReturnedToComposer): void {
  const { returnedSteering, clearReturnedSteering, returnedEdit, clearReturnedEdit, returnedDraft, clearReturnedDraft } = chat;
  useEffect(() => {
    if (returnedSteering) {
      input.current?.giveBack(returnedSteering);
      clearReturnedSteering();
    }
  }, [input, returnedSteering, clearReturnedSteering]);

  useEffect(() => {
    if (returnedEdit) {
      input.current?.giveBack(returnedEdit);
      clearReturnedEdit();
    }
  }, [input, returnedEdit, clearReturnedEdit]);

  useEffect(() => {
    if (returnedDraft) {
      input.current?.giveBack(returnedDraft.text, returnedDraft.files, { ...returnedDraft, focus: false });
      clearReturnedDraft();
    }
  }, [input, returnedDraft, clearReturnedDraft]);
}
