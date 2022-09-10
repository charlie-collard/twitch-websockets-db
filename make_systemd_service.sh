#!/bin/bash

cat <<- 'EOF' > /etc/systemd/system/twitch-websockets-db.service
[Unit]
Description="Starts up twitch websocket listener"

[Service]
User=charlie
WorkingDirectory=/home/charlie/workspace/twitch-websockets-db
ExecStart=bash -c "source credentials.sh && python3.9 db.py"
Restart=always
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable twitch-websockets-db.service
systemctl restart twitch-websockets-db.service
