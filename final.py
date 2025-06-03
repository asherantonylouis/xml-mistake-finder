import xml.etree.ElementTree as ET 
import csv
import os

# === CASE 1 FUNCTIONS ===

XML_FOLDER = r"C:\Users\user\Desktop\xmls_files1"

def parse_xml_with_lines(file_path):
    lines_by_path = {}
    parser = ET.iterparse(file_path, events=("start",))
    path_stack = []

    with open(file_path, "r", encoding="utf-8") as f:
        all_lines = f.readlines()

    for event, elem in parser:
        path_stack.append(elem.tag)
        path = "/" + "/".join(path_stack)
        lines_by_path[path] = {
            "attrib": dict(elem.attrib),
            "text": (elem.text or "").strip(),
            "line": find_line_with_tag(elem.tag, all_lines)
        }
        path_stack.pop()

    return lines_by_path

def find_line_with_tag(tag, lines):
    for line in lines:
        if f"<{tag}" in line or f"</{tag}>" in line:
            return line.strip()
    return ""

def compare_xml_with_lines(good, bad):
    differences = []

    for key in good:
        if key not in bad:
            
            differences.append(("-", "Tag missing", good[key]['line'], "-"))
        else:
            for attr in good[key]['attrib']:
                if attr not in bad[key]['attrib']:
                    differences.append((attr, "Attribute missing", good[key]['attrib'][attr], "-"))
                elif good[key]['attrib'][attr] != bad[key]['attrib'][attr]:
                    differences.append((attr, "Attribute mismatch", good[key]['attrib'][attr], bad[key]['attrib'][attr]))

            if good[key]['text'] != bad[key]['text']:
                differences.append(("(text)", "Text mismatch", good[key]['text'], bad[key]['text']))

    for key in bad:
        if key not in good:
        
            differences.append(("-", "Extra tag", "-", bad[key]['line']))

    return differences

def write_combined_differences_no_orderpair(all_differences, output_csv_file):
    fieldnames = ["attribute", "difference type", "wcs value", "microservice value"]
    with open(output_csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)
        for row in all_differences:
            writer.writerow(row)

def process_all_pairs_case1_no_orderpair(input_csv_file, output_csv_file):
    all_differences = []

    with open(input_csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        print("Detected CSV headers:", reader.fieldnames)

        if "good_xml" not in reader.fieldnames or "bad_xml" not in reader.fieldnames:
            raise ValueError("CSV must contain 'good_xml' and 'bad_xml' columns.")

        for row in reader:
            good_file = row["good_xml"]
            bad_file = row["bad_xml"]
            good_path = os.path.join(XML_FOLDER, good_file)
            bad_path = os.path.join(XML_FOLDER, bad_file)

            print(f" Comparing {good_file} vs {bad_file}...")

            try:
                good_elements = parse_xml_with_lines(good_path)
                bad_elements = parse_xml_with_lines(bad_path)
                diffs = compare_xml_with_lines(good_elements, bad_elements)

                all_differences.extend(diffs)
            except Exception as e:
                print(f" Error comparing {good_file} vs {bad_file}: {e}")

    write_combined_differences_no_orderpair(all_differences, output_csv_file)
    print(f"\n All differences written to: {output_csv_file}")


# === MAIN SWITCH ===

def main():
    print("Select option:")
    print("1 - Compare XML files from folder using CSV filenames (Case 1)")
    choice = input("Enter 1: ").strip()

    if choice == "1":
        input_csv = "input.csv"  
        output_csv = "all_differences.csv"
        process_all_pairs_case1_no_orderpair(input_csv, output_csv)
    else:
        print("Invalid choice or option not implemented. Exiting.")

if __name__ == "__main__":
    main()
