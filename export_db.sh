#!/bin/bash
# ============================================================
# Job Market Intelligence — Database Export Script
# Run this on your MAIN PC to export all scraped job data.
# The output file can then be transferred to Kali Linux.
# ============================================================

DB_NAME="job_market"
DB_USER="postgres"
OUTPUT_FILE="job_market_backup.sql"

echo ""
echo "============================================================"
echo "  Job Market Intelligence — Database Export"
echo "============================================================"
echo ""
echo ">>> Exporting database '$DB_NAME'..."

pg_dump -U "$DB_USER" -d "$DB_NAME" \
    --no-owner \
    --no-acl \
    --if-exists \
    --clean \
    -f "$OUTPUT_FILE"

if [ $? -eq 0 ]; then
    SIZE=$(du -sh "$OUTPUT_FILE" | cut -f1)
    echo ""
    echo "  Export complete!"
    echo "  File : $OUTPUT_FILE"
    echo "  Size : $SIZE"
    echo ""
    echo "  Next steps:"
    echo "  1. Copy '$OUTPUT_FILE' to a USB drive or transfer to Kali"
    echo "  2. Place it in the same folder as kali_setup.sh on Kali"
    echo "  3. Run: bash kali_setup.sh"
    echo "     The setup script will auto-detect and import the backup."
    echo ""
else
    echo ""
    echo "  Export failed. Make sure PostgreSQL is running and"
    echo "  the database '$DB_NAME' exists."
    echo ""
fi
