"""Artifacts: a workspace file's record, its published versions and comments.

``snapshot.py`` reads the file and the files it reads into a version, which
``manifest.py`` lists by walking what ``references.py`` finds in their text,
through ``reader.py``; ``grants.py`` signs the frames that open versions;
``comments.py`` carries comments to the agent in a chat send; ``events.py``
tells the owner's tabs what changed. The rows are the ``database/artifacts``
package.
"""
