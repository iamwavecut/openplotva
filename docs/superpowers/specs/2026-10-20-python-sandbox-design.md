# Python sandbox (`run_python`) design

## Intent

Chat users ask the bot for things a language model cannot draw or compute
reliably on its own: bar charts, floor plans, tables from a pasted CSV or a
crawled page, file conversions, audio and video edits, exact calculations.
The `run_python` dialog tool lets the model write a Python script, run it in
an isolated sandbox with a fixed package set, and send the files it creates
to the chat. The script stays in dialog history, so "make the bars blue" edits
the earlier script instead of starting from scratch.

## Goals

- Run model-written Python with no network, no host access, and bounded
  memory, CPU, time, processes and disk.
- Keep files between calls of one conversation for a short time, so a fix-up
  call can reuse inputs and outputs.
- Copy chat files into the sandbox and send output files back to the chat.
- Keep the code of every call in the persisted tool history.

## Non-goals

- Package installation at run time, network access from scripts, or
  long-running jobs.
- A vision check of the produced image before it is sent.

## Sandbox

The `openplotva-sandbox` binary is a small supervisor in its own container
image (`sandbox/Dockerfile`), built on the slim CPython image with nsjail
built from source and the packages from `sandbox/requirements.txt` installed
at build time. The package list covers data work (numpy, pandas, scipy,
sympy, statsmodels, scikit-learn, networkx, shapely), drawing (matplotlib,
seaborn, pillow, wordcloud, qrcode, graphviz, cairosvg), office and web
formats (openpyxl, xlrd, python-docx, pypdf, fpdf2, lxml, beautifulsoup4,
pyyaml) and media (ffmpeg-python with Debian's standard ffmpeg build).

Every call is one nsjail process (`sandbox/nsjail/python.cfg`):

| Limit | Value |
| --- | --- |
| Identity | uid/gid 65534 in a user namespace |
| Network | own network namespace without loopback |
| Memory | 256 MiB cgroup v2, no swap |
| CPU | 1 CPU, 30 s wall time, 60 s CPU rlimit |
| Processes | 16 pids |
| Files | 100 MiB per file, 128 descriptors |
| Filesystem | read-only system and venv mounts; writable `/home` only |
| Syscalls | kafel deny list for mount, namespace, ptrace, bpf, io_uring, module and keyring calls |

A session is a 100 MiB tmpfs mounted at `/home`. It closes 60 s after its
last call and at most 5 minutes after it opened; at most 4 sessions exist at
once, and a new session waits up to 10 s for a free slot. The matplotlib font
cache is built into the image and copied into each home.

The supervisor needs namespace, mount and cgroup privileges, so its container
runs privileged on an internal Docker network that only the bot shares. The
scripts themselves never see that network.

## Tool contract

`run_python(code, file_ids?, send_files?, caption?)`:

- `file_ids` reference chat attachments. The bot resolves them only when the
  file was seen in the same chat, downloads them (Telegram's 20 MB limit) and
  uploads them into the session under their original or derived names.
- After a clean exit, each path in `send_files` (up to 10) is read back and
  sent as a reply to the trigger message: images as photos or albums, `.gif`
  as an animation, `.mp4` as a video, `.mp3`/`.m4a` as audio, anything else as
  a document. A photo Telegram rejects is resent as a document. Sent messages
  go into chat history and the file metadata store.
- The result carries the termination, exit code, clipped stdout and stderr,
  the file list, input names and the delivery report. Failures map to
  `python_error`, `python_timeout`, `python_killed`, `sandbox_busy` and
  `sandbox_unavailable`, so the model can fix the script and retry.
- The stored tool result renders the call's `code` next to its output, which
  is how a follow-up turn finds the script to edit.
- The tool is offered only when `SANDBOX_URL` is set. SAFE runtime virtual
  dialogs run the script but withhold delivery.
