// @vitest-environment node
import { describe, it, expect } from 'vitest';
import i18n from '@/i18n';
import { NEW_ACCOUNT_WINDOW_MS, shouldAutoOpenWelcome, type AutoOpenInput } from '../welcome/autoOpen';
import {
  isLegacyOnboardingSearch,
  isWelcomeStep,
  legacyOnboardingTarget,
  openingStep,
  welcomeSteps,
  welcomeUrl,
} from '../welcome/paths';
import {
  answered,
  emptyProfile,
  pickBrokerages,
  profilePatch,
  profileRow,
  readProfile,
  resumeAnswers,
  unlistedSectors,
  type InvestingProfile,
} from '../welcome/profile';
import { brokerageNames, questionSummary } from '../welcome/summary';

const profile = (patch: Partial<InvestingProfile> = {}): InvestingProfile => ({ ...emptyProfile(), ...patch });

describe('readProfile / profilePatch', () => {
  const full = profile({
    investorType: 'professional',
    experience: '3_10y',
    helpWith: ['portfolio', 'ideas'],
    riskLevel: 'growth',
    horizon: 'years',
    markets: ['hk', 'us'],
    instruments: ['stock', 'etf'],
    approaches: ['fundamental', 'technical'],
    styles: ['value'],
    sectors: ['semiconductors'],
    depth: 'thorough',
    deliverable: 'visual',
    proactivity: 'when_needed',
  });

  it('round-trips through the shape the server stores', () => {
    expect(readProfile(profileRow(full))).toEqual(full);
    const own = profile({ sectors: ['biotech', 'technology', 'Health & Wellness'] });
    expect(readProfile(profileRow(own))).toEqual(own);
  });

  it('stores the risk level alone under risk, how the agent works under agent, and the rest under investment', () => {
    // The keys are the server's and the skills', so they are pinned here: what
    // the user wants to trade is still `asset_classes`, and the horizon moved
    // screens but not sections. The answer leads the output format's
    // ranking, which Settings orders whole.
    expect(profileRow(full)).toEqual({
      risk_preference: { risk_level: 'growth' },
      investment_preference: {
        investor_type: 'professional',
        experience: '3_10y',
        help_with: ['portfolio', 'ideas'],
        markets: ['hk', 'us'],
        asset_classes: ['stock', 'etf'],
        approaches: ['fundamental', 'technical'],
        styles: ['value'],
        horizon: 'years',
        sectors: ['semiconductors'],
        brokerages: [],
      },
      agent_preference: {
        research_depth: 'thorough',
        proactivity: 'when_needed',
        output_format: ['visual', 'markdown', 'text'],
      },
    });
  });

  it('reads a listed value spelled another way as that value, as the server folds it', () => {
    const read = readProfile({ investment_preference: { markets: [' US', 'hk', 'HK', 'Mars'], styles: ['Value'] } });
    expect(read.markets).toEqual(['us', 'hk']);
    expect(read.styles).toEqual(['value']);
  });

  it('stores the answer at the head of the output format ranking, the rest in the server order', () => {
    expect(profileRow(profile({ deliverable: 'markdown' })).agent_preference).toEqual({
      research_depth: null,
      proactivity: null,
      output_format: ['markdown', 'text', 'visual'],
    });
    expect(profileRow(profile({ deliverable: 'text' })).agent_preference.output_format).toEqual(['text', 'markdown', 'visual']);
    expect(profileRow(profile({ deliverable: 'visual' })).agent_preference.output_format).toEqual(['visual', 'markdown', 'text']);
    // Unanswered leaves it to the agent, as Settings' reset does.
    expect(profileRow(emptyProfile()).agent_preference.output_format).toBeNull();
  });

  it('reads the head of the ranking, and a row from before it the way the server migrated it', () => {
    const read = (agent: unknown) => readProfile({ agent_preference: agent });
    expect(read({ output_format: ['markdown', 'visual'] }).deliverable).toBe('markdown');
    expect(
      read({ research_depth: 'light', deliverable: 'text', proactivity: 'on_request', output_format: 'html' }),
    ).toEqual(profile({ depth: 'light', deliverable: 'visual', proactivity: 'on_request' }));
    expect(read({ output_format: 'html' }).deliverable).toBe('visual');
    expect(read({ deliverable: 'visual' }).deliverable).toBe('visual');
    expect(read({ deliverable: 'text', output_format: 'markdown' }).deliverable).toBe('text');
    expect(read({ output_format: 'markdown' }).deliverable).toBeNull();
  });

  it('reads a report saved as a deliverable, and any value it does not know, as unanswered', () => {
    const read = (agent: unknown) => readProfile({ agent_preference: agent });
    // Neither key ever held a report, and a ranking of formats it does not
    // know is the user's own words.
    expect(read({ deliverable: 'report' }).deliverable).toBeNull();
    expect(read({ output_format: ['pdf'] }).deliverable).toBeNull();
    // The depth an earlier build saved, among the rest.
    expect(read({ research_depth: 'deep', deliverable: 'slides', proactivity: 'hourly' })).toEqual(emptyProfile());
    expect(read({ research_depth: 3, deliverable: ['text'], proactivity: null })).toEqual(emptyProfile());
  });

  it('writes the ranking a new answer leads', () => {
    const text = { ...full, deliverable: 'text' as const };
    expect(profilePatch(text, full)).toEqual({ agent_preference: { output_format: ['text', 'markdown', 'visual'] } });
    expect(profilePatch({ ...full, deliverable: null }, full)).toEqual({ agent_preference: { output_format: null } });
  });

  it('sends only the agent answer that changed', () => {
    expect(profilePatch(full, full)).toBeNull();
    expect(profilePatch({ ...full, depth: 'light' }, full)).toEqual({ agent_preference: { research_depth: 'light' } });
    expect(profilePatch({ ...full, proactivity: 'on_request' }, full)).toEqual({
      agent_preference: { proactivity: 'on_request' },
    });
  });

  it('writes nothing over an order set in Settings while its head stays picked', () => {
    const saved = readProfile({
      risk_preference: { risk_level: 'growth' },
      investment_preference: { horizon: 'months' },
      agent_preference: {
        research_depth: 'adaptive',
        proactivity: 'when_needed',
        output_format: ['visual', 'text', 'markdown'],
      },
    });
    expect(saved.deliverable).toBe('visual');
    expect(profilePatch(saved, saved)).toBeNull();
    expect(profilePatch({ ...saved, horizon: 'years' }, saved)).toEqual({
      investment_preference: { horizon: 'years' },
    });
  });

  it('writes nothing over a row from before the ranking while its answer stays picked', () => {
    const saved = readProfile({
      risk_preference: { risk_level: 'growth' },
      investment_preference: { horizon: 'months' },
      agent_preference: {
        research_depth: 'adaptive',
        deliverable: 'text',
        proactivity: 'when_needed',
        output_format: 'markdown',
      },
    });
    expect(saved.deliverable).toBe('text');
    expect(profilePatch({ ...saved, horizon: 'years' }, saved)).toEqual({
      investment_preference: { horizon: 'years' },
    });
  });

  it('reads and writes the brokerages named when none is connected', () => {
    const named = profile({ brokerages: ['fidelity', 'other'] });
    expect(profileRow(named).investment_preference.brokerages).toEqual(['fidelity', 'other']);
    expect(readProfile(profileRow(named))).toEqual(named);
    // An unknown key is dropped and a repeat is read once.
    expect(
      readProfile({ investment_preference: { brokerages: ['schwab', 'nope', 'schwab', 4, 'none'] } }).brokerages,
    ).toEqual(['schwab', 'none']);
    expect(readProfile({ investment_preference: { brokerages: 'schwab' } }).brokerages).toEqual([]);
    expect(readProfile({ investment_preference: {} }).brokerages).toEqual([]);
  });

  it('sends the brokerages only when they changed', () => {
    expect(profilePatch(profile({ brokerages: ['ibkr'] }), profile({ brokerages: ['ibkr'] }))).toBeNull();
    expect(profilePatch(profile({ brokerages: ['ibkr', 'tiger'] }), profile())).toEqual({
      investment_preference: { brokerages: ['ibkr', 'tiger'] },
    });
    // Cleared by the user: sent empty.
    expect(profilePatch(profile(), profile({ brokerages: ['ibkr'] }))).toEqual({
      investment_preference: { brokerages: [] },
    });
    expect(profilePatch({ ...full, horizon: 'months' }, full)).not.toHaveProperty('investment_preference.brokerages');
  });

  it('reads what the previous questions saved', () => {
    // Asset classes and horizons from before the lists grew are all still on
    // them, so a returning user's answers come back prefilled.
    const read = readProfile({
      risk_preference: { risk_level: 'moderate' },
      investment_preference: {
        markets: ['us'],
        asset_classes: ['stock', 'option', 'crypto', 'bond'],
        styles: ['index'],
        horizon: 'days',
      },
    });
    expect(read).toEqual(
      profile({
        riskLevel: 'moderate',
        markets: ['us'],
        instruments: ['stock', 'option', 'crypto', 'bond'],
        styles: ['index'],
        horizon: 'days',
      }),
    );
  });

  it('sends only the answers that differ from the saved profile', () => {
    expect(profilePatch(full, full)).toBeNull();
    expect(profilePatch({ ...full, horizon: 'months' }, full)).toEqual({
      investment_preference: { horizon: 'months' },
    });
    expect(profilePatch({ ...full, riskLevel: 'moderate', markets: ['us', 'hk'] }, full)).toEqual({
      risk_preference: { risk_level: 'moderate' },
      investment_preference: { markets: ['us', 'hk'] },
    });
    expect(profilePatch({ ...full, investorType: 'student', approaches: ['macro'] }, full)).toEqual({
      investment_preference: { investor_type: 'student', approaches: ['macro'] },
    });
  });

  it('sends a cleared list empty, so it clears on the server too', () => {
    expect(profilePatch({ ...full, sectors: [], instruments: [], helpWith: [] }, full)).toEqual({
      investment_preference: { help_with: [], asset_classes: [], sectors: [] },
    });
  });

  it('keeps a sector the list does not name through a save that changes something else', () => {
    const saved = readProfile({
      risk_preference: { risk_level: 'growth' },
      investment_preference: { sectors: ['biotech', 'Technology'] },
    });
    expect(saved.sectors).toEqual(['biotech', 'technology']);
    expect(profilePatch({ ...saved, horizon: 'years' }, saved)).toEqual({
      investment_preference: { horizon: 'years' },
    });
    // And one that does change the sectors sends the unlisted one along.
    expect(profilePatch({ ...saved, sectors: [...saved.sectors, 'energy'] }, saved)).toEqual({
      investment_preference: { sectors: ['biotech', 'technology', 'energy'] },
    });
  });

  it('reads a spelling of a listed sector as it, and keeps any other as written', () => {
    const read = (sectors: unknown) => readProfile({ investment_preference: { sectors } }).sectors;
    expect(read(['Technology', 'Real Estate', 'biotech', 'biotech', '  ', 3])).toEqual([
      'technology',
      'real_estate',
      'biotech',
    ]);
    // A free-form value from before the list was a list.
    expect(read('Technology, clean energy')).toEqual(['technology', 'clean energy']);
    expect(unlistedSectors(['technology', 'biotech'], ['biotech', 'clean energy'])).toEqual([
      'biotech',
      'clean energy',
    ]);
  });

  it('leaves what the user has traded, which the flow no longer asks, as the server holds it', () => {
    const row = profileRow(readProfile({ investment_preference: { familiar_instruments: ['stock'] } }));
    expect(row.investment_preference).not.toHaveProperty('familiar_instruments');
  });

  it('drops only the values it cannot read, and leaves the old free-text keys alone', () => {
    const read = readProfile({
      risk_preference: { risk_tolerance: 'high', risk_level: 'reckless' },
      investment_preference: {
        investor_type: 'retail',
        experience: 5,
        help_with: ['news', 'gossip', 'news'],
        markets: ['us', 'mars', 'us', 7],
        asset_classes: ['etf', 'warrant'],
        approaches: 'technical',
        styles: 'value',
        horizon: 'long_term',
        sectors: ['energy', 'biotech'],
      },
    });
    expect(read).toEqual(
      profile({
        helpWith: ['news'],
        markets: ['us'],
        instruments: ['etf'],
        sectors: ['energy', 'biotech'],
      }),
    );
  });

  it('reads anything that is not a preferences row as unanswered', () => {
    expect(readProfile(null)).toEqual(emptyProfile());
    expect(readProfile('garbage')).toEqual(emptyProfile());
    expect(readProfile({ risk_preference: [], investment_preference: 3, agent_preference: 'deep' })).toEqual(
      emptyProfile(),
    );
  });
});

describe('resumeAnswers', () => {
  const seeded = profile({ riskLevel: 'growth', horizon: 'years', markets: ['us'] });

  it('keeps what the user answered and takes the newer saved value for the rest', () => {
    const answers = { ...seeded, riskLevel: 'aggressive' as const, markets: ['hk' as const] };
    const now = { ...seeded, markets: ['jp' as const], sectors: ['biotech'] };
    expect(resumeAnswers(answers, seeded, now)).toEqual({
      profile: { ...now, riskLevel: 'aggressive', markets: ['hk'] },
      base: { ...now, riskLevel: 'growth', markets: ['us'] },
    });
  });

  it('keeps the answers after the saved profile was reset, and still knows them as answers', () => {
    const answers = profile({ investorType: 'student', riskLevel: 'moderate', markets: ['cn'] });
    const once = resumeAnswers(answers, emptyProfile(), emptyProfile());
    expect(once.profile).toEqual(answers);
    expect(resumeAnswers(once.profile, once.base, profile({ markets: ['us'] })).profile).toEqual(answers);
  });
});

describe('openingStep', () => {
  const steps = welcomeSteps('new');

  it("opens on the draft's screen, else the URL's, else the first", () => {
    expect(openingStep(steps, ['markets', 'brokerage'])).toBe('markets');
    expect(openingStep(steps, [undefined, 'brokerage'])).toBe('brokerage');
    expect(openingStep(steps, [undefined, null])).toBe('brokerage');
    expect(openingStep(steps, [null, 'style'])).toBe('style');
    // A screen this visit does not have is passed over.
    expect(openingStep(welcomeSteps('edit'), ['brokerage', null])).toBe('experience');
  });

  it('opens at a question left unanswered before the screen it was sent to', () => {
    // A draft an older build left at markets never asked who the user is.
    const unanswered = (s: string) => s !== 'experience';
    expect(openingStep(steps, ['markets', null], unanswered)).toBe('experience');
    expect(openingStep(steps, [null, 'style'], unanswered)).toBe('experience');
    // Nothing to answer on the brokerage screen, and nothing ahead holds one back.
    const riskOpen = (s: string) => s !== 'risk';
    expect(openingStep(steps, ['experience', null], riskOpen)).toBe('experience');
    expect(openingStep(steps, ['style', null], riskOpen)).toBe('risk');
  });

  it('opens on the last question, how the agent works, only once the style before it is answered', () => {
    expect(openingStep(steps, ['agent', null])).toBe('agent');
    expect(openingStep(welcomeSteps('edit'), [null, 'agent'])).toBe('agent');
    expect(openingStep(steps, ['agent', null], (s) => s !== 'style')).toBe('style');
    // Unanswered itself, it is still where the visit opens: Start asks for it.
    expect(openingStep(steps, ['agent', null], (s) => s !== 'agent')).toBe('agent');
  });
});

describe('what each screen needs', () => {
  it('asks who the user is and for how long, and leaves what they want to trade optional', () => {
    expect(answered('experience', profile({ investorType: 'individual' }))).toBe(false);
    expect(answered('experience', profile({ experience: '1_3y' }))).toBe(false);
    expect(answered('experience', profile({ investorType: 'individual', experience: '1_3y' }))).toBe(true);
    expect(answered('experience', profile({ instruments: ['option'] }))).toBe(false);
  });

  it('asks for a risk level and a horizon together', () => {
    expect(answered('risk', profile({ riskLevel: 'moderate' }))).toBe(false);
    expect(answered('risk', profile({ horizon: 'months' }))).toBe(false);
    expect(answered('risk', profile({ riskLevel: 'moderate', horizon: 'months' }))).toBe(true);
  });

  it('asks for a market, and leaves what they want help with optional', () => {
    expect(answered('markets', profile({ markets: ['jp'] }))).toBe(true);
    expect(answered('markets', profile({ helpWith: ['ideas', 'research'] }))).toBe(false);
  });

  it('asks for an approach and a style, and leaves the sectors optional', () => {
    expect(answered('style', profile({ styles: ['index'] }))).toBe(false);
    expect(answered('style', profile({ approaches: ['macro'] }))).toBe(false);
    expect(answered('style', profile({ approaches: ['macro'], styles: ['index'] }))).toBe(true);
    expect(answered('style', profile({ sectors: ['energy'] }))).toBe(false);
  });

  it('asks how deep the agent goes, what an answer looks like and when it reaches out, all three', () => {
    const all = profile({ depth: 'light', deliverable: 'text', proactivity: 'on_request' });
    expect(answered('agent', all)).toBe(true);
    expect(answered('agent', { ...all, depth: null })).toBe(false);
    expect(answered('agent', { ...all, deliverable: null })).toBe(false);
    expect(answered('agent', { ...all, proactivity: null })).toBe(false);
    expect(answered('agent', { ...all, deliverable: 'visual' })).toBe(true);
    expect(answered('agent', { ...all, deliverable: 'markdown' })).toBe(true);
  });

  it('asks nothing of its own on the brokerage screen', () => {
    // Its way on is a connection, or the elsewhere turn, which needs nothing.
    expect(answered('brokerage', emptyProfile())).toBe(true);
  });

  it('keeps "No account yet" apart from the other brokerages, each clearing the other', () => {
    expect(pickBrokerages([], ['fidelity'])).toEqual(['fidelity']);
    expect(pickBrokerages(['fidelity'], ['fidelity', 'webull'])).toEqual(['fidelity', 'webull']);
    expect(pickBrokerages(['fidelity', 'webull'], ['fidelity', 'webull', 'none'])).toEqual(['none']);
    expect(pickBrokerages(['none'], ['none', 'webull'])).toEqual(['webull']);
    // Putting one down leaves the rest.
    expect(pickBrokerages(['fidelity', 'webull'], ['webull'])).toEqual(['webull']);
    expect(pickBrokerages(['none'], [])).toEqual([]);
  });
});

describe("the agent turn's answer", () => {
  const en = i18n.getFixedT('en-US');
  const zh = i18n.getFixedT('zh-CN');
  const agentSummary = (t: typeof en, p: InvestingProfile) =>
    questionSummary(t, t === zh ? 'zh-CN' : 'en-US', 'agent', p);

  // What the user's bubble reads once the last card folds: the three picks in
  // the order the card asks them, each in its card's own words.
  it('reads as how deep, what an answer looks like and when to reach out, in either language', () => {
    const p = profile({ depth: 'adaptive', deliverable: 'markdown', proactivity: 'when_needed' });
    expect(agentSummary(en, p)).toBe('Depends on the question · Summary with a file · When it matters');
    expect(agentSummary(zh, p)).toBe('看问题而定 · 摘要加文件 · 重要变动时');
    const other = profile({ depth: 'light', deliverable: 'visual', proactivity: 'on_request' });
    expect(agentSummary(en, other)).toBe('Quick answers · Charts and reports · Only when asked');
    expect(agentSummary(zh, other)).toBe('快速作答 · 图表和报告 · 仅在你要求时');
  });

  it('leaves out what is not answered yet rather than a gap', () => {
    expect(agentSummary(en, emptyProfile())).toBe('');
    expect(agentSummary(en, profile({ depth: 'thorough', proactivity: 'proactive' }))).toBe(
      'Full research · Regularly',
    );
    expect(agentSummary(en, profile({ deliverable: 'text' }))).toBe('Text in the chat');
  });

  it('reads a list of picks as one phrase, in the order the card asks them', () => {
    const p = profile({ approaches: ['technical', 'macro'], styles: ['value'], sectors: ['energy', 'clean_energy'] });
    expect(questionSummary(en, 'en-US', 'style', p)).toBe(
      'Technical analysis and Macro trends · Value · Energy and clean energy',
    );
  });
});

describe('brokerageNames', () => {
  const en = i18n.getFixedT('en-US');

  it('reads every chip picked, and only the accounts the agent is told of when used', () => {
    const keys = ['fidelity', 'other', 'none'] as const;
    // The answer reads as what was tapped: "Other" stands for itself until it
    // has a name, and "No account yet" is shown.
    expect(brokerageNames(en, [...keys], '  ')).toEqual(['Fidelity', 'Other', 'No account yet']);
    expect(brokerageNames(en, [...keys], ' Acme ')).toEqual(['Fidelity', 'Acme', 'No account yet']);
    // The kickoff names accounts: none for "No account yet", nor an unnamed other.
    expect(brokerageNames(en, [...keys], '  ', { used: true })).toEqual(['Fidelity']);
    expect(brokerageNames(en, [...keys], ' Acme ', { used: true })).toEqual(['Fidelity', 'Acme']);
  });
});

describe('shouldAutoOpenWelcome', () => {
  const NOW = Date.parse('2026-10-14T12:00:00Z');
  const fresh: AutoOpenInput = {
    pathname: '/dashboard',
    firstRun: true,
    onboardingCompleted: false,
    welcomeSkippedAt: null,
    createdAt: '2026-10-14T11:59:00Z',
    now: NOW,
  };

  it("opens on a new account's first dashboard visit", () => {
    expect(shouldAutoOpenWelcome(fresh)).toBe(true);
  });

  it('stays closed anywhere but the dashboard', () => {
    expect(shouldAutoOpenWelcome({ ...fresh, pathname: '/chat/t/abc' })).toBe(false);
    expect(shouldAutoOpenWelcome({ ...fresh, pathname: '/plugins' })).toBe(false);
  });

  it('stays closed on a later visit, a saved profile, or a skipped flow', () => {
    expect(shouldAutoOpenWelcome({ ...fresh, firstRun: false })).toBe(false);
    expect(shouldAutoOpenWelcome({ ...fresh, onboardingCompleted: true })).toBe(false);
    expect(shouldAutoOpenWelcome({ ...fresh, welcomeSkippedAt: 1 })).toBe(false);
  });

  it('leaves an existing account to the banner, and an unknown age closed', () => {
    const old = new Date(NOW - NEW_ACCOUNT_WINDOW_MS - 1).toISOString();
    expect(shouldAutoOpenWelcome({ ...fresh, createdAt: old })).toBe(false);
    expect(shouldAutoOpenWelcome({ ...fresh, createdAt: undefined })).toBe(false);
    expect(shouldAutoOpenWelcome({ ...fresh, createdAt: 'not a date' })).toBe(false);
  });
});

describe('welcome paths', () => {
  it('walks a new user from the brokerage through five questions, and an editor through the questions alone', () => {
    expect(welcomeSteps('new')).toEqual(['brokerage', 'experience', 'risk', 'markets', 'style', 'agent']);
    expect(welcomeSteps('edit')).toEqual(['experience', 'risk', 'markets', 'style', 'agent']);
  });

  it('builds the URLs that open the flow', () => {
    expect(welcomeUrl()).toBe('/welcome');
    expect(welcomeUrl({ step: 'brokerage' })).toBe('/welcome?step=brokerage');
    expect(welcomeUrl({ edit: true })).toBe('/welcome?edit=1');
    expect(isWelcomeStep('style')).toBe(true);
    expect(isWelcomeStep('agent')).toBe(true);
    expect(isWelcomeStep('interview')).toBe(false);
    // The hand-off is the last question's, not a screen of its own.
    expect(isWelcomeStep('start')).toBe(false);
  });

  it("sends the old sheet's parameter to the brokerage screen, keeping the rest", () => {
    expect(isLegacyOnboardingSearch('?onboarding=connect')).toBe(true);
    expect(isLegacyOnboardingSearch('?onboarding=other')).toBe(false);
    expect(legacyOnboardingTarget('?onboarding=connect&mcp_oauth=done')).toBe(
      '/welcome?mcp_oauth=done&step=brokerage',
    );
  });
});
