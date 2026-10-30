"""pdf_converter.py — interactive & CLI PDF converter (Markdown and/or Images).

Converts single PDF files or directories of PDFs into LLM-ready markdown (.md)
and/or page raster images (.png) using PyMuPDF (fitz) and pymupdf4llm.

Usage (CLI):
    python tools/admin/pdf_converter.py path/to/file.pdf --mode md
    python tools/admin/pdf_converter.py path/to/dir --mode img --dpi 300 --output path/to/out
    python tools/admin/pdf_converter.py --all --mode both --force

Usage (Interactive):
    python tools/admin/pdf_converter.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Project root: tools/admin/ -> tools/ -> repo root
ROOT = Path(__file__).resolve().parent.parent.parent
LOCAL_SOURCE = ROOT / "local" / "source" / "County Data Files"


def convert_pdf_to_markdown(doc: fitz.Document, md_path: Path, force: bool = False) -> tuple[bool, int, int]:
    """Convert an open PyMuPDF document to markdown with vector checkbox detection."""
    import pymupdf4llm  # noqa: PLC0415

    if md_path.exists() and not force:
        print(f"  SKIP MD   {md_path.name} (already exists, use --force to overwrite)")
        return True, 0, 0

    # Fast-Pass Vector Scanning for Checkboxes
    for page in doc:
        drawings = page.get_drawings()

        # 1. Density Fast-Pass: skip if overwhelmingly dense (maps, graphics)
        if len(drawings) > 1000:
            continue

        # 2. Geometric Strictness & O(N) Geometry Fast-Pass
        checkboxes = []
        for d in drawings:
            rects = [item for item in d.get("items", []) if item[0] == "re"]
            if len(rects) == 1 and d.get("type") == "s":
                rect = rects[0][1]
                # Checkboxes must be perfect squares (abs width-height < 2) 5-20px wide
                if 5 < rect.width < 20 and 5 < rect.height < 20 and abs(rect.width - rect.height) < 2:
                    checkboxes.append(rect)

        # Bypass thorough scan if zero shapes meet criteria
        if not checkboxes:
            continue

        # Extract checkmarks
        checkmarks = [
            d["rect"]
            for d in drawings
            if d.get("type") == "s" and len([item for item in d.get("items", []) if item[0] == "l"]) == 2
        ]

        # 3. Optimized Thorough Scan (Y-Coordinate Bounding)
        for box in checkboxes:
            is_checked = False
            for cm in checkmarks:
                if abs(cm.y0 - box.y0) < 10 or abs(cm.y1 - box.y1) < 10:
                    if box.intersects(cm):
                        is_checked = True
                        break

            text = "[x]" if is_checked else "[ ]"
            page.insert_text((box.x0, box.y1), text, fontsize=10, fontname="helv", color=(0, 0, 0))

    md_text = pymupdf4llm.to_markdown(doc)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(md_text, encoding="utf-8")

    md_bytes = md_path.stat().st_size
    return False, md_bytes, md_bytes


def convert_pdf_to_images(
    doc: fitz.Document, output_dir: Path, prefix: str, dpi: int = 300, force: bool = False
) -> tuple[int, int]:
    """Convert an open PyMuPDF document into PNG images per page."""
    output_dir.mkdir(parents=True, exist_ok=True)
    zoom_factor = dpi / 72.0
    matrix = fitz.Matrix(zoom_factor, zoom_factor)

    created_count = 0
    total_bytes = 0

    for page_index in range(len(doc)):
        output_file = output_dir / f"{prefix}_page_{page_index + 1}.png"
        if output_file.exists() and not force:
            continue

        page = doc[page_index]
        pixmap = page.get_pixmap(matrix=matrix, alpha=False)
        pixmap.save(str(output_file))
        created_count += 1
        total_bytes += output_file.stat().st_size

    return created_count, total_bytes


def prompt_user_for_inputs() -> tuple[list[Path], str, Path | None, int, bool]:
    """Interactive wizard prompting the user for conversion options."""
    print("=" * 60)
    print("            PDF Converter Setup Wizard")
    print("=" * 60)

    # 1. Target selection
    print("\n[Step 1/4] Select target file(s) or directory:")
    print(f"  1) Custom single PDF or directory path")
    print(f"  2) All PDFs in default source directory ({LOCAL_SOURCE})")
    target_choice = input("Choice (1-2, default=1): ").strip() or "1"

    targets: list[Path] = []
    if target_choice == "2":
        if not LOCAL_SOURCE.exists():
            print(f"ERROR: Default source path does not exist: {LOCAL_SOURCE}", file=sys.stderr)
            sys.exit(1)
        targets = sorted(LOCAL_SOURCE.rglob("*.pdf"))
    else:
        path_str = input("Enter path to PDF file or directory containing PDFs: ").strip().strip('"\'')
        p = Path(path_str).resolve()
        if not p.exists():
            print(f"ERROR: Path not found: {p}", file=sys.stderr)
            sys.exit(1)
        if p.is_dir():
            targets = sorted(p.rglob("*.pdf"))
        else:
            targets = [p]

    if not targets:
        print("No PDF files found to process.")
        sys.exit(0)

    # 2. Output format selection
    print("\n[Step 2/4] Choose conversion output format:")
    print("  1) Markdown (.md) — [Default]")
    print("  2) Images (.png per page)")
    print("  3) Both Markdown and Images")
    format_choice = input("Choice (1-3, default=1): ").strip() or "1"
    mode_map = {"1": "md", "2": "img", "3": "both"}
    mode = mode_map.get(format_choice, "md")

    dpi = 300
    if mode in ("img", "both"):
        dpi_str = input("\nEnter image rendering DPI (default=300): ").strip() or "300"
        try:
            dpi = int(dpi_str)
        except ValueError:
            dpi = 300

    # 3. Output location selection
    print("\n[Step 3/4] Select destination location:")
    print("  1) Save alongside original PDF files [Default]")
    print("  2) Save to custom output directory")
    dest_choice = input("Choice (1-2, default=1): ").strip() or "1"

    custom_out_dir: Path | None = None
    if dest_choice == "2":
        out_str = input("Enter output directory path: ").strip().strip('"\'')
        custom_out_dir = Path(out_str).resolve()

    # 4. Overwrite option
    print("\n[Step 4/4] Overwrite existing output files?")
    force_str = input("Overwrite existing files? (y/N): ").strip().lower()
    force = force_str in ("y", "yes")

    return targets, mode, custom_out_dir, dpi, force


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert PDF files to Markdown and/or Images (Interactive or Autonomous CLI).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  Interactive Mode:
    python tools/admin/pdf_converter.py

  CLI Autonomous Mode:
    python tools/admin/pdf_converter.py path/to/file.pdf --mode md
    python tools/admin/pdf_converter.py path/to/folder --mode img --dpi 300 --output path/to/output
    python tools/admin/pdf_converter.py --all --mode both --force
""",
    )

    parser.add_argument("target", nargs="?", type=Path, help="PDF file or directory to convert")
    parser.add_argument("--all", action="store_true", help=f"Process all PDFs under {LOCAL_SOURCE}")
    parser.add_argument(
        "--mode", choices=["md", "img", "both"], help="Conversion target format: md, img, or both"
    )
    parser.add_argument(
        "-o", "--output", type=Path, help="Custom output directory (default: alongside source PDF)"
    )
    parser.add_argument("--dpi", type=int, default=300, help="DPI for image rendering (default: 300)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output files")

    args = parser.parse_args()

    # Determine if CLI arguments were passed or if interactive wizard is needed
    is_autonomous = bool(args.target or args.all or args.mode)

    if is_autonomous:
        if args.all:
            targets = sorted(LOCAL_SOURCE.rglob("*.pdf"))
            if not targets:
                print(f"No PDFs found under {LOCAL_SOURCE}")
                return
        elif args.target:
            p = args.target.resolve()
            if not p.exists():
                print(f"ERROR: Path not found: {p}", file=sys.stderr)
                sys.exit(1)
            targets = sorted(p.rglob("*.pdf")) if p.is_dir() else [p]
        else:
            print("ERROR: Specify a target path or --all when running via CLI flags.", file=sys.stderr)
            sys.exit(1)

        mode = args.mode or "md"
        custom_out_dir = args.output.resolve() if args.output else None
        dpi = args.dpi
        force = args.force
    else:
        targets, mode, custom_out_dir, dpi, force = prompt_user_for_inputs()

    # Verify dependency imports before processing
    try:
        global fitz
        import fitz
    except ImportError:
        print("ERROR: pymupdf not installed. Run: uv add pymupdf", file=sys.stderr)
        sys.exit(1)

    print(f"\nProcessing {len(targets)} PDF file(s) [Mode: {mode.upper()}]...")

    total_md_files = 0
    total_img_files = 0

    for pdf_path in targets:
        print(f"\n► Processing: {pdf_path.name}")

        try:
            doc = fitz.open(pdf_path)
        except Exception as err:
            print(f"  ERROR opening PDF: {err}", file=sys.stderr)
            continue

        # Determine target directory and base filename
        out_dir = custom_out_dir if custom_out_dir else pdf_path.parent
        prefix = pdf_path.stem

        # 1. Convert to Markdown
        if mode in ("md", "both"):
            md_path = out_dir / f"{prefix}.md"
            skipped, _, md_bytes = convert_pdf_to_markdown(doc, md_path, force=force)
            if not skipped:
                total_md_files += 1
                print(f"  ✓ Markdown written: {md_path.name} ({md_bytes:,} bytes)")

        # 2. Convert to Images
        if mode in ("img", "both"):
            img_dir = out_dir / (prefix if custom_out_dir else "")
            count, img_bytes = convert_pdf_to_images(doc, img_dir, prefix=prefix, dpi=dpi, force=force)
            total_img_files += count
            if count > 0:
                print(f"  ✓ {count} image(s) rendered ({img_bytes:,} total bytes) at {dpi} DPI")

        doc.close()

    print("\n" + "=" * 60)
    print(f"Finished: {total_md_files} markdown file(s) created, {total_img_files} image(s) generated.")
    print("=" * 60)


if __name__ == "__main__":
    main()
