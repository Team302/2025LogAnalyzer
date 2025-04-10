import csv

def filter_autonomous_logs(input_path, output_path):
    with open(input_path, mode='r', newline='') as infile, open(output_path, mode='w', newline='') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)

        writer.writeheader()

        for row in reader:
            if row.get("RobotMode", "").strip().lower() == "autonomous":
                writer.writerow(row)