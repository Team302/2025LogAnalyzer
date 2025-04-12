import os
import subprocess
import sys
import argparse
from pathlib import Path


def convert_hoot_to_wpilog(hoot_dir, owlet_path=None, output_dir=None):
    """
    Convert all .hoot files in a directory to wpilog format using owlet.

    Args:
        hoot_dir (str): Directory containing .hoot files
        owlet_path (str): Path to the owlet executable (optional)
        output_dir (str): Directory for output files (optional)

    Returns:
        bool: True if successful, False otherwise
    """
    # Resolve paths
    hoot_dir_path = Path(hoot_dir).resolve()

    # Check if input directory exists
    if not hoot_dir_path.exists() or not hoot_dir_path.is_dir():
        print(f"Error: Hoot directory '{hoot_dir}' does not exist or is not a directory.")
        return False

    # Find owlet executable
    if owlet_path:
        owlet_exe = Path(owlet_path).resolve()
    else:
        # Look in current directory
        owlet_candidates = list(Path().glob("owlet*.exe"))
        if not owlet_candidates:
            print("Error: Owlet executable not found. Please specify with --owlet-path")
            return False
        owlet_exe = owlet_candidates[0]

    # Verify owlet exists
    if not owlet_exe.exists():
        print(f"Error: Owlet executable not found at '{owlet_exe}'")
        return False

    print(f"Using owlet: {owlet_exe}")

    # Set up output directory
    if output_dir:
        output_path = Path(output_dir).resolve()
    else:
        # Create wpilog subdirectory in the parent of hoot_dir
        output_path = hoot_dir_path.parent / "wpilog"

    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {output_path}")

    # Find all .hoot files
    hoot_files = list(hoot_dir_path.glob("*.hoot"))
    if not hoot_files:
        print(f"No .hoot files found in '{hoot_dir}'")
        return False

    print(f"Found {len(hoot_files)} .hoot files to convert")

    # Process each file
    successful = 0
    for hoot_file in hoot_files:
        try:
            print(f"Converting {hoot_file.name}...")

            # Determine output file name (same basename but .wpilog extension)
            output_file = output_path / f"{hoot_file.stem}.wpilog"

            # Build the owlet command using the correct syntax
            # ./owlet-25.2.0-windowsx86-64.exe --format=wpilog ./hoot/file.hoot output.wpilog
            cmd = [
                str(owlet_exe),
                "--format=wpilog",
                str(hoot_file),
                str(output_file)
            ]

            # Execute the command
            result = subprocess.run(cmd, capture_output=True, text=True)

            # Check for errors
            if result.returncode != 0:
                print(f"Error converting {hoot_file.name}:")
                print(result.stderr)
                continue

            print(f"Successfully converted {hoot_file.name} to {output_file.name}")
            successful += 1

        except Exception as e:
            print(f"Error processing {hoot_file.name}: {str(e)}")

    print(f"Conversion complete. Successfully converted {successful} out of {len(hoot_files)} files.")
    return successful > 0


def main():
    parser = argparse.ArgumentParser(description="Convert CTRE Hoot log files to WPILog format")
    parser.add_argument("hoot_dir", help="Directory containing .hoot files to convert")
    parser.add_argument("--owlet-path", help="Path to the owlet executable")
    parser.add_argument("--output-dir", help="Directory for output .wpilog files (default: 'wpilog' subdirectory)")

    args = parser.parse_args()

    success = convert_hoot_to_wpilog(
        args.hoot_dir,
        args.owlet_path,
        args.output_dir
    )

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())