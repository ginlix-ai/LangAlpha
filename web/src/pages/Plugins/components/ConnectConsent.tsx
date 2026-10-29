import { AnimatePresence } from '@/lib/framer';
import { connectAsks } from '../brokerages';
import { useConnectQuestion } from '../connectQuestion';
import { BrokerageConsentDialog } from './BrokerageConsentDialog';
import { LeaveForSignIn } from './LeaveForSignIn';

/**
 * The question a connect raises before the vendor's page, for whichever
 * surface started it, so none of them reaches a brokerage's consent screen one
 * question quieter than another; the lifecycle holds the request until it is
 * answered.
 *
 * A brokerage asks what the connection may do. Anything else that waits on a
 * sign-in only says that the app is about to leave for the vendor's page, and
 * what does not happen until the user is back.
 */
export function ConnectConsent() {
  const question = useConnectQuestion();
  const request = question?.request;
  return (
    <AnimatePresence>
      {question && request && !connectAsks(request.vendor) && (
        <LeaveForSignIn
          key={`leave:${request.name}`}
          request={request}
          pending={question.starting}
          onConfirm={() => question.answer()}
          onCancel={question.cancel}
        />
      )}
      {question && request && connectAsks(request.vendor) && (
        <BrokerageConsentDialog
          // Keyed by row, because the dialog seeds its toggles once from the
          // grant it opened on. Reused across rows it would show the previous
          // row's answer.
          key={request.name}
          vendor={request.vendor}
          name={request.name}
          granted={request.granted}
          pending={question.starting}
          onConfirm={question.answer}
          onCancel={question.cancel}
        />
      )}
    </AnimatePresence>
  );
}

export default ConnectConsent;
