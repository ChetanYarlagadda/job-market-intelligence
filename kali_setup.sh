#!/bin/bash
# ============================================================
# Job Market Intelligence Engine — Kali Linux Setup Script
# Run this once on your Kali machine to set everything up.
# Usage: bash kali_setup.sh
# ============================================================

set -e

REPO_URL="https://github.com/ChetanYarlagadda/job-market-intelligence.git"
APP_DIR="$HOME/job-market-intelligence"
DB_NAME="job_market"
DB_USER="postgres"
DB_PASS="Kafka@2104"
APP_PORT=8502
CURRENT_USER=$(whoami)

echo ""
echo "============================================================"
echo "  Job Market Intelligence Engine — Kali Linux Setup"
echo "  User: $CURRENT_USER"
echo "  Install dir: $APP_DIR"
echo "============================================================"
echo ""

# ── Prompt for OpenAI API key ─────────────────────────────────
echo ">>> Enter your OpenAI API key (starts with sk-):"
read -r OPENAI_KEY
if [ -z "$OPENAI_KEY" ]; then
    echo "    No key entered. You can add it later in: $APP_DIR/scrape_config.json"
    OPENAI_KEY=""
fi

# ── Step 1: System dependencies ──────────────────────────────
echo ""
echo ">>> [1/8] Installing system dependencies..."
sudo apt-get update -q
sudo apt-get install -y \
    python3 python3-pip python3-venv \
    postgresql postgresql-contrib \
    git curl wget \
    libpq-dev gcc python3-dev \
    libxml2-dev libxslt1-dev zlib1g-dev
echo "    OK: System dependencies installed"

# ── Step 2: Clone repo ────────────────────────────────────────
echo ""
echo ">>> [2/8] Cloning repository..."
if [ -d "$APP_DIR" ]; then
    echo "    Already exists — pulling latest..."
    cd "$APP_DIR" && git pull
else
    git clone "$REPO_URL" "$APP_DIR"
fi
cd "$APP_DIR"
echo "    OK: Repository at $APP_DIR"

# ── Step 3: Python virtual environment ───────────────────────
echo ""
echo ">>> [3/8] Setting up Python virtual environment..."
python3 -m venv "$APP_DIR/venv"
source "$APP_DIR/venv/bin/activate"
pip install --upgrade pip -q
pip install -r requirements.txt -q
echo "    OK: Python environment ready"

# ── Step 4: PostgreSQL ────────────────────────────────────────
echo ""
echo ">>> [4/8] Setting up PostgreSQL..."
sudo systemctl start postgresql
sudo systemctl enable postgresql

sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'" \
    | grep -q 1 || sudo -u postgres psql -c "CREATE DATABASE $DB_NAME;"

sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname='$DB_USER'" \
    | grep -q 1 || sudo -u postgres psql -c "CREATE USER $DB_USER WITH PASSWORD '$DB_PASS';"

sudo -u postgres psql -c "ALTER USER $DB_USER WITH PASSWORD '$DB_PASS';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;"
sudo -u postgres psql -c "ALTER DATABASE $DB_NAME OWNER TO $DB_USER;"

PG_HBA=$(sudo -u postgres psql -t -c "SHOW hba_file;" | tr -d ' ')
if ! sudo grep -q "md5" "$PG_HBA" 2>/dev/null; then
    sudo sed -i 's/^local\s*all\s*postgres\s*peer/local   all             postgres                                md5/' "$PG_HBA"
    sudo systemctl reload postgresql
fi
echo "    OK: PostgreSQL ready (db: $DB_NAME)"

# ── Step 5: Config file ───────────────────────────────────────
echo ""
echo ">>> [5/8] Creating app config..."
cat > "$APP_DIR/scrape_config.json" <<EOF
{
  "roles": [
    "Data Engineer",
    "Data Analyst",
    "Data Scientist",
    "Machine Learning Engineer",
    "AI Engineer"
  ],
  "priority_locations": [
    "San Francisco, CA", "Seattle, WA", "New York, NY",
    "Austin, TX", "Los Angeles, CA", "Chicago, IL",
    "Boston, MA", "Denver, CO", "Atlanta, GA",
    "Washington, DC", "Dallas, TX", "San Diego, CA",
    "Charlotte, NC", "Raleigh, NC", "Minneapolis, MN",
    "Miami, FL", "Phoenix, AZ", "Nashville, TN",
    "Philadelphia, PA", "Remote"
  ],
  "schedule_enabled": true,
  "schedule_interval_hours": 1,
  "openai_api_key": "$OPENAI_KEY"
}
EOF
echo "    OK: Config created (auto-scrape every 1 hour)"

# ── Step 6: Directories ───────────────────────────────────────
echo ""
echo ">>> [6/8] Creating required directories..."
mkdir -p "$APP_DIR/logs" "$APP_DIR/data/raw" "$APP_DIR/data/processed"
echo "    OK: Directories ready"

# ── Step 6b: Import existing data backup (if present) ─────────
BACKUP_FILE="$(dirname "$0")/job_market_backup.sql"
if [ -f "$BACKUP_FILE" ]; then
    echo ""
    echo ">>> Found job_market_backup.sql — importing existing data..."
    BACKUP_SIZE=$(du -sh "$BACKUP_FILE" | cut -f1)
    echo "    Backup size: $BACKUP_SIZE"
    sudo -u postgres psql -d "$DB_NAME" -f "$BACKUP_FILE" -q
    JOB_COUNT=$(sudo -u postgres psql -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM jobs;" 2>/dev/null | tr -d ' ' || echo "unknown")
    echo "    OK: Data imported — $JOB_COUNT jobs loaded"
else
    echo ""
    echo "    INFO: No backup file found — starting with empty database."
    echo "    Tip: To import existing data, place job_market_backup.sql"
    echo "         next to this script and run it again."
fi

# ── Step 7: systemd service ───────────────────────────────────
echo ""
echo ">>> [7/8] Installing systemd service..."
sudo tee /etc/systemd/system/jobintel.service > /dev/null <<EOF
[Unit]
Description=Job Market Intelligence API
After=network.target postgresql.service
Requires=postgresql.service

[Service]
Type=simple
User=$CURRENT_USER
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/venv/bin/uvicorn api:app --host 0.0.0.0 --port $APP_PORT
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable jobintel
sudo systemctl restart jobintel
sleep 4

if sudo systemctl is-active --quiet jobintel; then
    echo "    OK: Service is running and starts on boot"
else
    echo "    FAILED: Check logs with: sudo journalctl -u jobintel -n 50"
fi

# ── Step 8: Firewall ──────────────────────────────────────────
echo ""
echo ">>> [8/8] Checking firewall..."
if sudo ufw status 2>/dev/null | grep -q "Status: active"; then
    sudo ufw allow $APP_PORT/tcp
    echo "    OK: Port $APP_PORT allowed"
else
    echo "    OK: UFW not active — port $APP_PORT is open"
fi

# ── Summary ───────────────────────────────────────────────────
LOCAL_IP=$(hostname -I | awk '{print $1}')
echo ""
echo "============================================================"
echo "  SETUP COMPLETE!"
echo "============================================================"
echo ""
echo "  Status         : $(sudo systemctl is-active jobintel)"
echo "  Local access   : http://localhost:$APP_PORT"
echo "  Main PC access : http://$LOCAL_IP:$APP_PORT"
echo ""
echo "  Open http://$LOCAL_IP:$APP_PORT on your main PC browser"
echo ""
echo "  Commands:"
echo "    Status   : sudo systemctl status jobintel"
echo "    Logs     : sudo journalctl -u jobintel -f"
echo "    Restart  : sudo systemctl restart jobintel"
echo "    Update   : cd $APP_DIR && git pull && sudo systemctl restart jobintel"
echo ""
echo "============================================================"
