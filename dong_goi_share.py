import zipfile
from pathlib import Path

BASE_DIR = Path(__file__).parent.resolve()
OUTPUT_ZIP = BASE_DIR / "WebVideoCast_Studio_v3.0_Share.zip"

FILES_TO_INCLUDE = [
    "server.py",
    "requirements.txt",
    "1_CAI_DAT_LAN_DAU.bat",
    "2_CHAY_WEBVIDEOCAST_STUDIO.bat",
    "run_cast_studio.bat",
    "HUONG_DAN_SU_DUNG.txt",
    "README.md",
    "static/index.html",
    "static/tv.html",
    "chrome_extension/manifest.json",
    "chrome_extension/background.js",
    "chrome_extension/popup.html",
    "chrome_extension/popup.js",
]


def build_share_zip(target_path: Path = OUTPUT_ZIP) -> Path:
    with zipfile.ZipFile(target_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for rel_path in FILES_TO_INCLUDE:
            full_path = BASE_DIR / rel_path
            if full_path.exists():
                arcname = f"WebVideoCast_Studio_v3.0/{rel_path}"
                zf.write(full_path, arcname=arcname)
    return target_path


if __name__ == "__main__":
    out = build_share_zip()
    print(f"[OK] Da dong goi ban chia se sach se tai: {out} ({out.stat().st_size:,} bytes)")
