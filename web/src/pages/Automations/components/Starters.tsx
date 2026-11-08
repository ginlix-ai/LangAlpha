import React from 'react';
import { useTranslation } from 'react-i18next';
import { ArrowRight } from 'lucide-react';
import { STARTER_ORDER, templatesById, type TemplateId } from '../utils/templates';
import { Cadence } from './CadenceMarks';
import './Starters.css';

const STARTERS = templatesById(STARTER_ORDER);

/**
 * The page before anything is set up. Each starting point is listed with
 * when it would run, drawn in the marks the page uses once it has content,
 * so choosing one is choosing a cadence as much as a topic. Below, the ground
 * the feed will cover stays visible as the one empty canvas.
 */
export default function Starters({ onPick }: { onPick: (id: TemplateId) => void }) {
  const { t } = useTranslation();
  return (
    <div className="automations-zero">
      <h2 className="automations-kicker">{t('automation.startFrom')}</h2>
      <ul className="automations-ledger">
        {STARTERS.map((tpl) => (
          <li key={tpl.id}>
            <button type="button" className="automations-starter automations-ledger-row" onClick={() => onPick(tpl.id)}>
              <span className="automations-starter-when">
                <Cadence template={tpl} />
              </span>
              <span className="min-w-0">
                <span className="automations-ledger-name">{t(tpl.nameKey)}</span>
                <span className="automations-ledger-desc">{t(tpl.descriptionKey)}</span>
              </span>
              <ArrowRight className="automations-ledger-go h-3.5 w-3.5" aria-hidden="true" />
            </button>
          </li>
        ))}
      </ul>
      <div className="automations-zero-field dot-grid">
        <p>{t('automation.feedWillFill')}</p>
      </div>
    </div>
  );
}
