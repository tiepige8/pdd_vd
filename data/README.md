# Data folder

Runtime files are written here and should **not** be committed.

Ignored files:
- `config.json`, `tokens.json`, `schedule.json`, `upload_state.json`
- `upload.log`, `covers/`
- video files (see `../video/`)

Sample configs to copy:
- `config.sample.json` → `config.json`
- `schedule.sample.json` → `schedule.json`

Example:
```bash
cp data/config.sample.json data/config.json
cp data/schedule.sample.json data/schedule.json
```
