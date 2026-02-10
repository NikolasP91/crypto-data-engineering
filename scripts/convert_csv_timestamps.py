from pathlib import Path
import pandas as pd

processed_dir = Path(__file__).resolve().parent.parent / "data" / "processed"
# find latest CSV
csv_files = sorted(processed_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
if not csv_files:
    print("No processed CSV files found.")
    raise SystemExit(1)

csv_path = csv_files[0]
backup_path = csv_path.with_suffix(csv_path.suffix + ".bak")
print(f"Processing: {csv_path}\nBackup: {backup_path}")

df = pd.read_csv(csv_path)

# Convert epoch ms columns if present
for col in ["open_time", "close_time"]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], unit='ms', utc=True).dt.strftime('%Y-%m-%dT%H:%M:%SZ')

# Convert extracted_at_utc of form YYYYMMDDTHHMMSSZ
if "extracted_at_utc" in df.columns:
    try:
        df["extracted_at_utc"] = pd.to_datetime(df["extracted_at_utc"], format='%Y%m%dT%H%M%SZ', utc=True).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    except Exception:
        pass

# Write backup and overwrite csv
csv_path.replace(backup_path)
# write with same name
df.to_csv(csv_path, index=False)
print("Done. Preview:")
print(df.head(10).to_csv(index=False))
