"""Files an OS adds when it packs a folder, never part of what was packed.

One predicate for every reader of what a user packed (plugin packages, skill
uploads, a skill's file listing), so an upload is judged the same whichever
door it comes through. Each reader adds its own residue (caches, licenses,
hidden files) on top.
"""

from collections.abc import Sequence


def is_os_litter(segments: Sequence[str]) -> bool:
    """Finder's Compress writes a ``__MACOSX/`` tree of ``._*`` resource forks
    beside the folder it zipped, and a ``.DS_Store`` wherever a window was
    opened; macOS ``tar`` writes the same ``._*`` files inline. Kept, the tree
    reads as a second top-level folder, so the one wrapping the upload is not
    found where it is.
    """
    return bool(segments) and (
        "__MACOSX" in segments
        or segments[-1] == ".DS_Store"
        or segments[-1].startswith("._")
    )
