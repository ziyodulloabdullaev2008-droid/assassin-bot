Server deploy

Quick way
- Run `python prepare_server_bundle.py`
- Upload `dist/server_bundle` to the server
- On the server run `chmod +x run.sh`
- Start with `./run.sh`

What is inside
- Bot code
- `config.local.json`
- `users/` with sessions and broadcast configs
- Linux helper `run.sh`
- `deploy/systemd/assassin-bot.service`

Important
- `config.local.json` contains secrets; upload it only to your own server
- The bundle skips `.venv`, `tests`, `__pycache__`, `.pyc`, `*.session-journal`, and backup json files

Recommended server steps
1. Install Python 3.11+ and pip
2. Upload `dist/server_bundle` anywhere, for example `/opt/assassin-bot`
3. Run:
   - `cd /opt/assassin-bot`
   - `chmod +x run.sh`
   - `./run.sh`

Systemd
- Example unit: `deploy/systemd/assassin-bot.service`
- Copy it to `/etc/systemd/system/assassin-bot.service`
- Edit `WorkingDirectory`, `User`, and `ExecStart`
- Then run:
  - `sudo systemctl daemon-reload`
  - `sudo systemctl enable assassin-bot`
  - `sudo systemctl start assassin-bot`
  - `sudo systemctl status assassin-bot`
