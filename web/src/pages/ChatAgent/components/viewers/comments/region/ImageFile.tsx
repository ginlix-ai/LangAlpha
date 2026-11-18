import { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { ViewerComments, type ViewerCommentsTarget } from '../ViewerComments';
import { scrollParent } from '../scrollParent';
import { imageSource } from './regionPictures';
import { regionSpots, type RegionSurfaces } from './regionSpots';

/** The image as the one surface boxes and points are drawn on: its box is the image's, at any size it is shown. */
function imageSurfaces(frame: HTMLElement): RegionSurfaces {
  return {
    points: true,
    at(x, y) {
      const r = frame.getBoundingClientRect();
      return x >= r.left && x <= r.right && y >= r.top && y <= r.bottom ? { el: frame } : null;
    },
    // A box on a page is a PDF's, never this image's.
    of: (page) => (page ? null : { el: frame }),
  };
}

/**
 * An image in the panel. A click opens it whole; with the owner's comments
 * (`comments`) in comment mode, a click drops a point on it and a drag draws
 * a box, and a box's picture is written for the agent as its comment is saved.
 */
export function ImageFile({ src, alt, comments, onOpen }: {
  src: string;
  alt: string;
  comments: ViewerCommentsTarget | null;
  onOpen: () => void;
}) {
  const { t } = useTranslation();
  const [frame, setFrame] = useState<HTMLDivElement | null>(null);
  const [img, setImg] = useState<HTMLImageElement | null>(null);
  const scroller = useMemo(() => frame && scrollParent(frame), [frame]);
  const strategy = useMemo(() => (frame ? regionSpots(imageSurfaces(frame)) : null), [frame]);
  const pictures = useMemo(() => (img ? imageSource(img, src) : null), [img, src]);

  return (
    <>
      <div ref={setFrame} className="relative inline-block max-w-full align-top">
        <img ref={setImg} src={src} alt={alt} className="block max-w-full rounded cursor-pointer" onClick={onOpen} />
      </div>
      {comments && (
        <ViewerComments
          scroller={scroller}
          content={frame}
          strategy={strategy}
          target={comments}
          modeHint={t('pageViewer.comments.modeHintImage')}
          pictures={pictures}
          // The box drawn stays in view.
          keepSpotClear
        />
      )}
    </>
  );
}
