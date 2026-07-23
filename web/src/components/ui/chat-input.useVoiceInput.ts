import { useCallback, useEffect, useRef, useState } from 'react';
import type { Dispatch, SetStateAction } from 'react';
import { useTranslation } from 'react-i18next';
import { useToast } from './use-toast';

export const speechSupported = typeof window !== 'undefined' && !!(window.SpeechRecognition || window.webkitSpeechRecognition);

/** Voice input (Web Speech API) for the chat composer: appends interim +
 * final transcripts onto the message, stops when a turn starts loading and
 * on unmount. `stopListening` is for the host's type/IME interrupt paths. */
export function useVoiceInput({ message, setMessage, isLoading }: {
  message: string;
  setMessage: Dispatch<SetStateAction<string>>;
  isLoading?: boolean;
}) {
  const { t, i18n } = useTranslation();
  const { toast } = useToast();

  const [isListening, setIsListening] = useState(false);
  const recognitionRef = useRef<SpeechRecognition | null>(null);
  const isStartingRef = useRef(false);
  const finalTranscriptRef = useRef('');
  const baseMessageRef = useRef('');
  const messageRef = useRef(message);

  // Sync message ref
  useEffect(() => { messageRef.current = message; }, [message]);

  // Stop recognition when loading starts (ghost text fix)
  useEffect(() => {
    if (isLoading && recognitionRef.current) {
      recognitionRef.current.stop();
      recognitionRef.current = null;
      setIsListening(false);
    }
  }, [isLoading]);

  const toggleListening = useCallback(() => {
    if (isStartingRef.current) return;

    // ALWAYS stop existing instance if it exists (prevents orphaned instances on rapid clicks)
    if (recognitionRef.current) {
      recognitionRef.current.stop();
      recognitionRef.current = null;
    }

    if (isListening) {
      setIsListening(false);
      return;
    }

    const SpeechRecognitionAPI = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!SpeechRecognitionAPI) {
      console.warn("Speech recognition is not supported in this browser.");
      return;
    }

    try {
      const recognition = new SpeechRecognitionAPI();
      recognition.continuous = true;
      recognition.interimResults = true;
      // Derived purely from current UI locale
      recognition.lang = i18n.language.startsWith('zh') ? 'zh-CN' : 'en-US';

      // Capture message BEFORE starting recognition
      const startMessage = messageRef.current.trim();
      baseMessageRef.current = startMessage ? startMessage + ' ' : '';

      recognition.onstart = () => {
        setIsListening(true);
        isStartingRef.current = false;
        finalTranscriptRef.current = ''; // Reset for new session
      };

      recognition.onresult = (event: SpeechRecognitionEvent) => {
        if (recognitionRef.current !== recognition) return;
        let interimTranscript = '';
        // Start from resultIndex to avoid re-processing or duplicating old results
        for (let i = event.resultIndex; i < event.results.length; ++i) {
          const result = event.results[i];
          if (result.isFinal) {
            finalTranscriptRef.current += result[0].transcript;
          } else {
            interimTranscript += result[0].transcript;
          }
        }
        setMessage(baseMessageRef.current + finalTranscriptRef.current + interimTranscript);
      };

      recognition.onerror = (event: SpeechRecognitionErrorEvent) => {
        if (recognitionRef.current !== recognition) return;
        if (event.error !== 'no-speech' && event.error !== 'aborted') {
          console.error('Speech recognition error:', event.error);
          if (event.error === 'not-allowed') {
            toast({
              title: t('chat.voice.permissionDenied'),
              variant: 'destructive',
            });
          } else if (event.error === 'service-not-allowed' || event.error === 'network') {
            toast({
              title: t('chat.voice.serviceError'),
              variant: 'destructive',
            });
          }
        }
        setIsListening(false);
        isStartingRef.current = false;
        recognitionRef.current = null;
      };

      recognition.onend = () => {
        if (recognitionRef.current !== recognition) return;
        isStartingRef.current = false; // Release lock in case onstart never fired
        setIsListening(false);
        recognitionRef.current = null;
      };

      recognitionRef.current = recognition;
      isStartingRef.current = true;
      recognition.start();
    } catch (err) {
      console.error('Failed to start speech recognition:', err);
      isStartingRef.current = false;
      recognitionRef.current = null; // Prevent stale ref if start() throws
      setIsListening(false);
    }
  }, [isListening, toast, t, i18n.language, setMessage]);

  // Clean up recognition on unmount
  useEffect(() => {
    return () => {
      if (recognitionRef.current) {
        recognitionRef.current.abort(); // Terminate immediately without firing callbacks
        recognitionRef.current = null;
      }
    };
  }, []);

  const stopListening = useCallback(() => {
    recognitionRef.current?.stop();
  }, []);

  return { isListening, toggleListening, stopListening };
}
