import os
import csv
import argparse
import glob


def process_csv_file(input_file, columns_to_keep=None, output_file=None):
    """
    Process a CSV file to:
    1. Filter rows where RobotMode=autonomous
    2. Apply timestamp offset based on the first autonomous timestamp
    3. Filter columns if specified

    Args:
        input_file (str): Path to the input CSV file
        columns_to_keep (list, optional): List of column names to keep
        output_file (str, optional): Path to save the output CSV file

    Returns:
        bool: True if processing was successful, False otherwise
    """
    if not output_file:
        base_name = os.path.basename(input_file)
        name, ext = os.path.splitext(base_name)
        output_file = f"{name}-auton-filtered{ext}"

    try:
        with open(input_file, 'r', newline='') as infile:
            # Read the CSV file
            reader = csv.DictReader(infile)

            # Verify that required columns exist
            all_columns = reader.fieldnames
            if not all_columns:
                print(f"Error: No columns found in {input_file}")
                return False

            if 'RobotMode' not in all_columns:
                print(f"Error: 'RobotMode' column not found in {input_file}")
                return False

            if 'timestamp' not in all_columns:
                print(f"Error: 'timestamp' column not found in {input_file}")
                return False

            # Determine which columns to keep
            if columns_to_keep:
                # Ensure RobotMode and timestamp are included
                if 'RobotMode' not in columns_to_keep:
                    columns_to_keep.append('RobotMode')
                if 'timestamp' not in columns_to_keep:
                    columns_to_keep.append('timestamp')

                # Check for missing columns
                missing_columns = [col for col in columns_to_keep if col not in all_columns]
                if missing_columns:
                    print(f"Warning: These columns were not found: {', '.join(missing_columns)}")
                    # Remove missing columns
                    columns_to_keep = [col for col in columns_to_keep if col in all_columns]
            else:
                columns_to_keep = all_columns

            # Add adjusted_time to the output columns
            if 'adjusted_time' not in columns_to_keep:
                columns_to_keep.append('adjusted_time')

            # First pass: Find the first autonomous timestamp
            infile.seek(0)  # Reset file pointer
            next(reader)  # Skip header
            first_auton_timestamp = None

            for row in reader:
                if row['RobotMode'] == 'autonomous':
                    timestamp_str = row['timestamp'].strip()
                    try:
                        # Parse timestamp in seconds.milliseconds format (00.000)
                        first_auton_timestamp = float(timestamp_str)
                        break
                    except ValueError:
                        print(f"Warning: Could not parse timestamp '{timestamp_str}' as float")
                        continue

            if first_auton_timestamp is None:
                print(f"Warning: No autonomous mode rows found in {input_file}")
                return False

            # Second pass: Process the file
            infile.seek(0)  # Reset file pointer
            reader = csv.DictReader(infile)

            # Create output file and writer
            with open(output_file, 'w', newline='') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=columns_to_keep)
                writer.writeheader()

                filtered_rows = []
                for row in reader:
                    if row['RobotMode'] == 'autonomous':
                        # Extract only the columns we want to keep
                        filtered_row = {col: row.get(col, '') for col in columns_to_keep if col != 'adjusted_time'}

                        # Parse timestamp and calculate offset
                        timestamp_str = row['timestamp'].strip()
                        try:
                            timestamp = float(timestamp_str)
                            # Calculate offset in seconds (for first autonomous row, this will be 0)
                            offset = timestamp - first_auton_timestamp
                            # Add adjusted timestamp with the same precision as input
                            filtered_row['adjusted_time'] = f"{offset:.3f}"
                            filtered_rows.append(filtered_row)
                        except ValueError:
                            print(f"Warning: Could not parse timestamp '{timestamp_str}' as float")

                # Write filtered rows to output file
                writer.writerows(filtered_rows)

            print(f"Successfully processed {input_file}")
            print(f"Filtered data saved to {output_file}")
            print(f"Found {len(filtered_rows)} rows in autonomous mode")
            return True

    except FileNotFoundError:
        print(f"Error: File {input_file} not found")
        return False
    except PermissionError:
        print(f"Error: Permission denied when writing to {output_file}")
        return False
    except Exception as e:
        print(f"Error processing {input_file}: {str(e)}")
        return False


def process_directory(directory_path, columns_to_keep=None, pattern="*.csv"):
    """
    Process all CSV files in a directory.

    Args:
        directory_path (str): Path to the directory containing CSV files
        columns_to_keep (list, optional): List of column names to keep
        pattern (str, optional): Glob pattern to match CSV files

    Returns:
        tuple: (number of files processed, number of failures)
    """
    if not os.path.isdir(directory_path):
        print(f"Error: {directory_path} is not a valid directory")
        return 0, 0

    csv_files = glob.glob(os.path.join(directory_path, pattern))
    if not csv_files:
        print(f"No CSV files found in {directory_path} matching pattern '{pattern}'")
        return 0, 0

    success_count = 0
    failure_count = 0

    for file_path in csv_files:
        print(f"\nProcessing {file_path}...")
        success = process_csv_file(file_path, columns_to_keep)
        if success:
            success_count += 1
        else:
            failure_count += 1

    return success_count, failure_count


def main():
    parser = argparse.ArgumentParser(description='Process CSV files to filter autonomous mode data.')
    parser.add_argument('input', help='Input CSV file or directory containing CSV files')
    parser.add_argument('--columns', '-c', nargs='+', help='List of columns to keep (default: all columns)')
    parser.add_argument('--pattern', '-p', default="*.csv",
                        help='File pattern when processing a directory (default: *.csv)')

    args = parser.parse_args()

    if os.path.isdir(args.input):
        print(f"Processing directory: {args.input}")
        success, failure = process_directory(args.input, args.columns, args.pattern)
        print(f"\nSummary: Processed {success} files successfully, {failure} files failed")
    elif os.path.isfile(args.input):
        print(f"Processing file: {args.input}")
        success = process_csv_file(args.input, args.columns)
        if success:
            print("Processing completed successfully")
        else:
            print("Processing failed")
    else:
        print(f"Error: {args.input} is neither a valid file nor directory")


if __name__ == '__main__':
    main()