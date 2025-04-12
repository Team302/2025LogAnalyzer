import argparse
import csv
import os
import struct
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any


@dataclass
class Entry:
    timestamp: float
    name: str
    value: Any


@dataclass
class WPILogMetadata:
    entry_names: Dict[int, str] = None
    entry_types: Dict[int, str] = None
    start_time: Optional[float] = None

    def __post_init__(self):
        if self.entry_names is None:
            self.entry_names = {}
        if self.entry_types is None:
            self.entry_types = {}


def read_string(data: bytes, offset: int) -> Tuple[str, int]:
    """Read a string from the data buffer starting at offset."""
    length = struct.unpack('<H', data[offset:offset + 2])[0]
    offset += 2
    string_data = data[offset:offset + length]
    offset += length
    return string_data.decode('utf-8'), offset


def parse_value(data: bytes, offset: int, entry_type: str) -> Tuple[Any, int]:
    """Parse a value of the specified type from the data."""
    if entry_type == "boolean":
        value = struct.unpack('?', data[offset:offset + 1])[0]
        offset += 1
    elif entry_type == "int":
        value = struct.unpack('<q', data[offset:offset + 8])[0]
        offset += 8
    elif entry_type == "float":
        value = struct.unpack('<f', data[offset:offset + 4])[0]
        offset += 4
    elif entry_type == "double":
        value = struct.unpack('<d', data[offset:offset + 8])[0]
        offset += 8
    elif entry_type == "string":
        value, offset = read_string(data, offset)
    elif entry_type == "boolean[]":
        length = struct.unpack('<I', data[offset:offset + 4])[0]
        offset += 4
        value = []
        for _ in range(length):
            v = struct.unpack('?', data[offset:offset + 1])[0]
            value.append(v)
            offset += 1
    elif entry_type == "int[]":
        length = struct.unpack('<I', data[offset:offset + 4])[0]
        offset += 4
        value = []
        for _ in range(length):
            v = struct.unpack('<q', data[offset:offset + 8])[0]
            value.append(v)
            offset += 8
    elif entry_type == "float[]":
        length = struct.unpack('<I', data[offset:offset + 4])[0]
        offset += 4
        value = []
        for _ in range(length):
            v = struct.unpack('<f', data[offset:offset + 4])[0]
            value.append(v)
            offset += 4
    elif entry_type == "double[]":
        length = struct.unpack('<I', data[offset:offset + 4])[0]
        offset += 4
        value = []
        for _ in range(length):
            v = struct.unpack('<d', data[offset:offset + 8])[0]
            value.append(v)
            offset += 8
    elif entry_type == "string[]":
        length = struct.unpack('<I', data[offset:offset + 4])[0]
        offset += 4
        value = []
        for _ in range(length):
            s, offset = read_string(data, offset)
            value.append(s)
    else:
        raise ValueError(f"Unknown type: {entry_type}")

    return value, offset


def parse_wpilog(file_path: str) -> Tuple[WPILogMetadata, List[Entry]]:
    """Parse a WPILog file and extract entries."""
    metadata = WPILogMetadata()
    entries = []

    with open(file_path, 'rb') as f:
        data = f.read()

    offset = 0
    header = data[offset:offset + 6]
    offset += 6

    if header != b'WPILOG':
        raise ValueError("Invalid WPILog file, header doesn't match 'WPILOG'")

    # Read until EOF
    while offset < len(data):
        try:
            entry_type = data[offset]
            offset += 1

            timestamp = struct.unpack('<q', data[offset:offset + 8])[0] / 1000000  # Convert to seconds
            offset += 8

            if entry_type == 0:  # Control entry
                control_type = data[offset]
                offset += 1

                if control_type == 0:  # Start
                    metadata.start_time = timestamp
                elif control_type == 1:  # Finish
                    pass
                else:
                    print(f"Unknown control type: {control_type}")

            elif entry_type == 1:  # Metadata entry
                entry_name, offset = read_string(data, offset)
                metadata_entry, offset = read_string(data, offset)

                # Special handling for entry names and types
                if entry_name == "entry":
                    entry_id, name = metadata_entry.split(';', 1)
                    metadata.entry_names[int(entry_id)] = name
                elif entry_name == "type":
                    entry_id, type_name = metadata_entry.split(';', 1)
                    metadata.entry_types[int(entry_id)] = type_name

            elif entry_type == 2:  # Data entry
                entry_id = struct.unpack('<H', data[offset:offset + 2])[0]
                offset += 2

                if entry_id in metadata.entry_names and entry_id in metadata.entry_types:
                    entry_name = metadata.entry_names[entry_id]
                    entry_type = metadata.entry_types[entry_id]

                    value, offset = parse_value(data, offset, entry_type)
                    entries.append(Entry(timestamp, entry_name, value))
                else:
                    print(f"Unknown entry ID: {entry_id}")
                    break
            else:
                print(f"Unknown entry type: {entry_type}")
                break
        except Exception as e:
            print(f"Error parsing file at offset {offset}: {e}")
            break

    return metadata, entries


def generate_csv(entries: List[Entry], output_file: str, period_ms: float = 20.0):
    """Generate a CSV file with a fixed period from the entries."""
    # Group entries by name
    entries_by_name = {}
    for entry in entries:
        if entry.name not in entries_by_name:
            entries_by_name[entry.name] = []
        entries_by_name[entry.name].append((entry.timestamp, entry.value))

    # Find the start and end time
    if not entries:
        print("No entries found")
        return

    start_time = min(entry.timestamp for entry in entries)
    end_time = max(entry.timestamp for entry in entries)

    # Create a list of timestamps with the specified period
    period_s = period_ms / 1000.0
    timestamps = []
    current_time = start_time
    while current_time <= end_time:
        timestamps.append(current_time)
        current_time += period_s

    # Prepare the CSV data
    csv_data = []
    header = ["Timestamp"]

    # Sort entries by name for consistent output
    sorted_names = sorted(entries_by_name.keys())
    header.extend(sorted_names)

    # Create interpolation function for each entry type
    for timestamp in timestamps:
        row = [timestamp]

        for name in sorted_names:
            # Find the closest value before and after the current timestamp
            entry_data = entries_by_name[name]

            # Find the last entry before or at the current timestamp
            before = None
            for t, v in entry_data:
                if t <= timestamp:
                    before = (t, v)
                else:
                    break

            # Find the first entry after the current timestamp
            after = None
            for t, v in reversed(entry_data):
                if t >= timestamp:
                    after = (t, v)
                else:
                    break

            # Use the closest entry or interpolate
            if before is None and after is None:
                value = ""
            elif before is None:
                value = after[1]
            elif after is None:
                value = before[1]
            elif timestamp - before[0] <= after[0] - timestamp:
                value = before[1]
            else:
                value = after[1]

            # Handle arrays by converting to string representation
            if isinstance(value, list):
                value = str(value)

            row.append(value)

        csv_data.append(row)

    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Write to CSV
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(csv_data)

    return {
        "total_entries": len(entries),
        "unique_types": len(sorted_names),
        "start_time": start_time,
        "end_time": end_time,
        "duration": end_time - start_time,
        "rows": len(csv_data)
    }


def process_file(input_file: str, output_file: str, period_ms: float = 20.0):
    """Process a single WPILog file and convert it to CSV."""
    try:
        start_time = time.time()
        metadata, entries = parse_wpilog(input_file)
        parse_time = time.time() - start_time

        if metadata.start_time:
            start_datetime = datetime.fromtimestamp(metadata.start_time)
        else:
            start_datetime = "Unknown"

        stats = generate_csv(entries, output_file, period_ms)

        total_time = time.time() - start_time

        return {
            "success": True,
            "input_file": input_file,
            "output_file": output_file,
            "log_start_time": start_datetime,
            "parse_time": parse_time,
            "total_time": total_time,
            **stats
        }
    except Exception as e:
        return {
            "success": False,
            "input_file": input_file,
            "output_file": output_file,
            "error": str(e)
        }


def process_directory(input_dir: str, output_dir: str, period_ms: float = 20.0):
    """Process all WPILog files in a directory and convert them to CSV files."""
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Get all WPILog files in the input directory
    wpilog_files = []
    for file in os.listdir(input_dir):
        if file.lower().endswith('.wpilog'):
            wpilog_files.append(file)

    if not wpilog_files:
        print(f"No WPILog files found in {input_dir}")
        return

    print(f"Found {len(wpilog_files)} WPILog files in {input_dir}")

    results = []
    for i, file in enumerate(wpilog_files, 1):
        input_file = os.path.join(input_dir, file)
        output_file = os.path.join(output_dir, os.path.splitext(file)[0] + '.csv')

        print(f"[{i}/{len(wpilog_files)}] Processing {file}...")

        result = process_file(input_file, output_file, period_ms)
        results.append(result)

        if result["success"]:
            print(f"  Converted to {output_file}")
            if "total_entries" in result:
                print(f"  Entries: {result['total_entries']}, CSV Rows: {result['rows']}")
                print(
                    f"  Time range: {result['start_time']:.3f}s to {result['end_time']:.3f}s (Duration: {result['duration']:.3f}s)")
            print(f"  Processing time: {result['total_time']:.2f} seconds")
        else:
            print(f"  Error: {result['error']}")

        print()

    # Print summary
    successful = sum(1 for r in results if r["success"])
    failed = len(results) - successful

    print(f"Conversion complete: {successful} succeeded, {failed} failed")

    if successful > 0:
        avg_time = sum(r["total_time"] for r in results if r["success"]) / successful
        print(f"Average processing time: {avg_time:.2f} seconds per file")

    return results


def main():
    parser = argparse.ArgumentParser(description='Convert WPILog files to CSV with fixed period')
    parser.add_argument('input', help='Input WPILog file or directory')
    parser.add_argument('output', help='Output CSV file or directory')
    parser.add_argument('--period', type=float, default=20.0, help='Period in milliseconds (default: 20.0)')

    args = parser.parse_args()

    # Check if input is a directory or a file
    if os.path.isdir(args.input):
        print(f"Processing directory: {args.input}")
        process_directory(args.input, args.output, args.period)
    else:
        print(f"Processing file: {args.input}")
        result = process_file(args.input, args.output, args.period)

        if result["success"]:
            print(f"Converted to {args.output}")
            print(f"Entries: {result['total_entries']}, CSV Rows: {result['rows']}")
            print(
                f"Time range: {result['start_time']:.3f}s to {result['end_time']:.3f}s (Duration: {result['duration']:.3f}s)")
            print(f"Processing time: {result['total_time']:.2f} seconds")
        else:
            print(f"Error: {result['error']}")


if __name__ == "__main__":
    main()