import { useCallback, useContext } from 'react';
import { UNSAFE_NavigationContext, useNavigate, type NavigateOptions } from 'react-router';

/**
 * Edits the query string from what the URL holds now, not from what this
 * component last rendered with.
 *
 * react-router's functional `setSearchParams` hands the updater the params of
 * the render it came from, and the router moves its location inside a
 * transition, so a state change made in the same tick renders first, on the
 * old location. Two writers in one page (the market page dropping the link's
 * params as it applies them, and its chat dropping `?thread` for the
 * workspace that link moved to) then each start from the old URL, and the
 * second puts back what the first took out. The router's history always holds
 * the URL as of the last write, so each edit starts there.
 */
export function useEditSearchParams(): (edit: (params: URLSearchParams) => void, options?: NavigateOptions) => void {
  const { navigator } = useContext(UNSAFE_NavigationContext);
  const navigate = useNavigate();
  return useCallback((edit, options) => {
    // Browser and memory histories both carry their current location; the
    // `Navigator` type leaves it out.
    const live = (navigator as { location?: { search?: string } }).location?.search ?? window.location.search;
    const was = new URLSearchParams(live);
    const next = new URLSearchParams(was);
    edit(next);
    // An edit that changes nothing writes nothing, so it cannot race another.
    if (next.toString() === was.toString()) return;
    navigate(`?${next}`, options);
  }, [navigator, navigate]);
}
