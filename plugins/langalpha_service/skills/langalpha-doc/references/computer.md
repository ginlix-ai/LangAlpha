# The Computer

## Processes

- **ExecuteCode** starts a fresh `python3` process each call, in your workspace folder.
- To import your own modules, run the script from Bash (`python <task>/run.py`) or put its folder on `sys.path`.
- A missing package is not reliably installed for you: `pip install` it in Bash, then run again.
- **Bash** starts a fresh shell each call, in your workspace folder. `cd` and `export` do not carry over, and stderr comes back merged with stdout.
- ExecuteCode calls wait for a free CPU slot, one per CPU of the computer, shared with other threads, subagents and sibling workspaces, so parallel ExecuteCode calls queue. Bash does not. A run moved to the background gives its slot up.
- `os.cpu_count()` and `nproc` report the host machine, not your share. `OMP_NUM_THREADS` holds your CPU count; size worker pools from it.
- One disk serves every workspace on the computer, and `df -h /` shows it. The low-disk warning comes late, so check before writing large data.

## What survives

| Event | Kept | Lost |
|---|---|---|
| Stop, then start | the whole disk: files, installed packages, `/tmp` | running processes, background commands included |
| Archive, then restore (after about a week stopped) | the whole disk, after a wait of up to five minutes | the same as a stop |
| Rebuild (the sandbox is lost, the resource tier changes, or it was deleted after long dormancy) | backed-up workspace folders | installed packages, the computer root, `/tmp`, everything under `.agents/threads/` |

- The computer stops on its own about 50 to 60 minutes after the last turn started, unless the user keeps it always on. A turn, background subagent or background command still running holds it up.
- Each workspace folder is backed up after every completed turn, and on a best-effort basis before a stop. Files written after a turn ends wait for the next completed turn.
- The backup skips dependency and cache folders (`.venv`, `venv`, `node_modules`, `vendor`, `.git`, `.cache`, `__pycache__` and similar), compiled files (`.pyc`, `.so`, `.dylib`, `.o`), `.agents/threads/`, `.agents/tools/`, and single files over the size limit (at least 100 MB). A cloned repository comes back without its history, and a virtual environment does not come back usable under any name, so keep a `requirements.txt` or a setup script in the workspace and reinstall when an import fails after a rebuild.
- After a rebuild your workspace folder is restored before your turn starts; saved tool results follow during the turn. A sibling workspace is restored when it is next opened, so `../<sibling>/` can be missing until then.

## Preinstalled

Python with pandas, numpy, scipy, scikit-learn, statsmodels, matplotlib, seaborn, plotly, openpyxl, python-docx, python-pptx, pypdf, pdfplumber, reportlab, beautifulsoup4, lxml, playwright and yfinance; `rg`, `jq`, `git`, `gh`, `pandoc`, poppler, `qpdf`, LibreOffice (Writer, Calc, Impress, Draw), `uv` and Node. Check before installing.

## Data-server tools from code

- Import with `from tools.<server> import <function>`, from ExecuteCode or from a script Bash runs. A character that is not valid in a Python name becomes `_`, and a parameter named like a Python keyword (including `type` and `match`) gets a trailing `_`.
- A folder or module named `tools` next to your script hides the wrappers.
- Only servers enabled for this workspace have wrappers and docs under `.agents/tools/`; importing any other raises `ModuleNotFoundError`. `../.agents/tools/docs/` covers every server any workspace on the computer enabled, so a doc there does not mean you can call it here.
- A built-in data server reports a failure as a value, `{"error": ..., "detail": ...}`, not an exception, so check for it; other servers return their own error text. A protocol or connection failure raises `RuntimeError`.
- Calls to a local (stdio) server run one at a time across the whole computer; remote servers take calls in parallel. A local server idle for 15 minutes restarts on its next call, which can take half a minute.
- Never edit the wrappers under `.agents/tools/`: they are regenerated, and sibling workspaces share them.

## Secrets

`vault` reads one file for the whole computer, holding every secret the user saved under Plugins, Secrets, so code sees the same secrets from any folder, `/tmp` and the computer root included. If it says the workspace still names its own vault, that clears once the workspace starts again.
