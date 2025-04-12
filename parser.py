import csv

def filter_autonomous_logs(input_path, output_path):
    rows = []
    autonomous_start_time = None

    # First pass: collect rows and identify the first autonomous timestamp
    with open(input_path, mode='r', newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            if row.get("RobotMode", "").strip().lower() == "autonomous":
                timestamp = float(row["Timestamp"])
                if autonomous_start_time is None:
                    autonomous_start_time = timestamp
                rows.append(row)

    if autonomous_start_time is None:
        print("No autonomous mode found in the input file.")
        return

    # Second pass: write only filtered and adjusted rows
    with open(output_path, mode='w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=rows[0].keys())
        writer.writeheader()


        for row in rows:
            adjusted_time = float(row["Timestamp"]) - autonomous_start_time
            row["Timestamp"] = f"{adjusted_time:.3f}"
            writer.writerow(row)
