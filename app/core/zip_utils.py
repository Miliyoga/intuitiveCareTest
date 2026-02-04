from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

def zip_single_file(input_file: Path, zip_path: Path, arcname: str | None = None) -> Path:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    if arcname is None:
        arcname = input_file.name

    with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as zf:
        zf.write(input_file, arcname=arcname)

    return zip_path
