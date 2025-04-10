import argparse
import os
from parser import filter_autonomous_logs

def generate_output_filename(input_file):
    base, ext = os.path.splitext(input_file)
    return f"{base}_autonomous{ext}"

def main():
    parser = argparse.ArgumentParser(description="Filter CSV rows where RobotMode == 'autonomous'")
    parser.add_argument("input_file", help="Path to input CSV file")
    args = parser.parse_args()

    input_file = args.input_file
    output_file = generate_output_filename(input_file)

    filter_autonomous_logs(input_file, output_file)
    print(f"Filtered CSV saved as '{output_file}'.")

if __name__ == '__main__':
    main()
