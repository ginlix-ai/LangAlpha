/**
 * The gallery's paging arithmetic: how many cards fit, which page is showing,
 * and how a page change is animated.
 *
 * Pure viewport bookkeeping with no product logic, so it is kept away from the
 * gallery's queries and mutations. The two refs it returns are deliberately not
 * state: the grid height is locked to stop the pagination dots from moving, and
 * the slide direction has to be readable in the same commit that changes the
 * page.
 */
import React, { useCallback, useEffect, useRef, useState } from 'react';

import { getIsMobileSnapshot } from '@/hooks/useIsMobile';

import { WORKSPACE_CARD_HEIGHT } from './cardMetrics';

export const DEFAULT_PAGE_SIZE = 8;

interface UseGalleryPagingOptions {
  isSearching: boolean;
}

/** How many cards fit in `height`, at the current breakpoint. */
function computePageSizeFromHeight(height: number): number {
  const isMobile = getIsMobileSnapshot();
  const columns = isMobile ? 1 : 2;
  const gap = isMobile ? 12 : 24;
  const cardHeight = WORKSPACE_CARD_HEIGHT;
  const gridBottomMargin = isMobile ? 12 : 24;
  const available = height - gridBottomMargin;
  const rows = Math.max(1, Math.floor((available + gap) / (cardHeight + gap)));
  return Math.max(2, columns * rows);
}

export function useGalleryPaging({ isSearching }: UseGalleryPagingOptions) {
  const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE);
  const [currentPage, setCurrentPage] = useState(0);
  // The measured column mounts and unmounts (a loading screen and reorder mode
  // both replace it), so the node arrives as state: an effect keyed on a ref
  // would run once against `null` and never see the element appear.
  const [scrollEl, setScrollEl] = useState<HTMLDivElement | null>(null);
  const setScrollContainer = useCallback((el: HTMLDivElement | null) => setScrollEl(el), []);
  const slideDirectionRef = useRef(0); // 1 = forward, -1 = back
  const skipInitialAnimRef = useRef(true); // skip slide animation on first render
  const gridHeightRef = useRef<number | null>(null); // locked grid height for consistent dot placement
  const touchStartRef = useRef<{ x: number; y: number; t: number } | null>(null);

  const goToPage = useCallback((page: number) => {
    gridHeightRef.current = null;
    setCurrentPage((prev) => {
      slideDirectionRef.current = page > prev ? 1 : -1;
      return page;
    });
  }, []);

  // Reveal a mutation whose result lands at the top of the list (pin, duplicate):
  // slide backwards to page 0 and release the locked grid height.
  const snapToFirstPage = useCallback(() => {
    slideDirectionRef.current = -1;
    gridHeightRef.current = null;
    setCurrentPage(0);
  }, []);

  /** Back to page 0 without a direction, for a sort or mode change. */
  const resetToFirstPage = useCallback(() => {
    gridHeightRef.current = null;
    setCurrentPage(0);
  }, []);

  /** Step back a page, for a delete that emptied the current one. */
  const stepBackPage = useCallback(() => {
    setCurrentPage((p) => {
      if (p === 0) return p;
      slideDirectionRef.current = -1;
      return p - 1;
    });
  }, []);

  /** Read-and-clear: the first render skips the slide-in, later ones do not. */
  const takeSkipInitialAnim = useCallback(() => {
    const skip = skipInitialAnimRef.current;
    if (skip) skipInitialAnimRef.current = false;
    return skip;
  }, []);

  // Swipe gesture handlers for mobile pagination. `totalPages` is the
  // caller's, because the page count comes from the list query's total and
  // this hook deliberately holds no server state.
  const swipeHandlers = useCallback((totalPages: number) => ({
    onTouchStart: (e: React.TouchEvent) => {
      const touch = e.touches[0];
      touchStartRef.current = { x: touch.clientX, y: touch.clientY, t: Date.now() };
    },
    onTouchEnd: (e: React.TouchEvent) => {
      if (!touchStartRef.current || isSearching || totalPages <= 1) return;
      const touch = e.changedTouches[0];
      const dx = touch.clientX - touchStartRef.current.x;
      const dy = touch.clientY - touchStartRef.current.y;
      const dt = Date.now() - touchStartRef.current.t;
      touchStartRef.current = null;

      // Require: horizontal distance > 50px, more horizontal than vertical, within 500ms
      if (Math.abs(dx) > 50 && Math.abs(dx) > Math.abs(dy) * 1.5 && dt < 500) {
        if (dx < 0 && currentPage < totalPages - 1) {
          goToPage(currentPage + 1);
        } else if (dx > 0 && currentPage > 0) {
          goToPage(currentPage - 1);
        }
      }
    },
  }), [isSearching, currentPage, goToPage]);

  // Scroll to top when page changes
  useEffect(() => {
    scrollEl?.scrollTo({ top: 0, behavior: 'smooth' });
  }, [currentPage, scrollEl]);

  // Dynamic page size: measure how many cards fit in the scroll container.
  // The pagination container is always rendered (visibility:hidden when unused)
  // so this height is stable and needs no reserve.
  useEffect(() => {
    const el = scrollEl;
    // Null while the loading screen or reorder mode holds the column: those
    // are the same states an explicit "should I measure" flag would name.
    if (!el) return;

    let debounceTimer: ReturnType<typeof setTimeout> | null = null;

    const handleResize = () => {
      if (debounceTimer) clearTimeout(debounceTimer);
      debounceTimer = setTimeout(() => {
        const newSize = computePageSizeFromHeight(el.clientHeight);
        setPageSize((prev) => (prev === newSize ? prev : newSize));
      }, 200);
    };

    // Measure after a frame to ensure layout is settled
    requestAnimationFrame(() => {
      setPageSize(computePageSizeFromHeight(el.clientHeight));
    });

    const observer = new ResizeObserver(handleResize);
    observer.observe(el);

    return () => {
      observer.disconnect();
      if (debounceTimer) clearTimeout(debounceTimer);
    };
  }, [scrollEl]);

  // Reset page and grid height when page size changes
  const prevPageSizeRef = useRef(DEFAULT_PAGE_SIZE);
  useEffect(() => {
    if (prevPageSizeRef.current !== pageSize) {
      prevPageSizeRef.current = pageSize;
      resetToFirstPage();
    }
  }, [pageSize, resetToFirstPage]);

  const isFirstPage = currentPage === 0;

  return {
    pageSize,
    currentPage,
    isFirstPage,
    // Page 0 reserves one slot for the flash card; later pages are full.
    limit: isSearching ? 100 : isFirstPage ? pageSize - 1 : pageSize,
    offset: isSearching ? 0 : isFirstPage ? 0 : (pageSize - 1) + (currentPage - 1) * pageSize,
    goToPage,
    snapToFirstPage,
    resetToFirstPage,
    stepBackPage,
    takeSkipInitialAnim,
    setScrollContainer,
    slideDirectionRef,
    gridHeightRef,
    swipeHandlers,
  };
}
