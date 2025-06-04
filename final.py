import xml.etree.ElementTree as ET
import csv
import os
import mysql.connector

# === SETTINGS ===
XML_FOLDER = r"C:\Users\user\Desktop\xmls_files1"
EXCLUDED_ATTRIBUTES_FILE = "excluded_attributes.csv"


# === COMMON UTILITIES ===
def load_excluded_attributes(file_path=EXCLUDED_ATTRIBUTES_FILE):
    excluded = set()
    try:
        with open(file_path, newline='', encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            if not reader.fieldnames or 'attribute' not in reader.fieldnames:
                print(f"⚠️ Invalid or missing 'attribute' column in: {file_path}")
                return excluded
            for row in reader:
                attr = row['attribute'].strip()
                if attr:
                    excluded.add(attr)
    except FileNotFoundError:
        print(f"⚠️ Excluded attribute file not found: {file_path}")
    return excluded


# === CASE 1: File-based XML comparison ===
def parse_xml_with_lines(file_path):
    elements = {}
    parser = ET.iterparse(file_path, events=("start",))
    path_stack = []

    with open(file_path, "r", encoding="utf-8") as f:
        all_lines = f.readlines()

    for event, elem in parser:
        path_stack.append(elem.tag)
        path = "/" + "/".join(path_stack)
        if path not in elements:
            elements[path] = {
                "attrib": dict(elem.attrib),
                "text": (elem.text or "").strip()
            }
        path_stack.pop()
    return elements


def compare_xml_elements(good, bad, excluded_attrs):
    differences = []
    for key in good:
        if key not in bad:
            good_text = f"<{key.split('/')[-1]}>{good[key]['text']}</{key.split('/')[-1]}>"
            differences.append(("-", "Tag missing", good_text, "-"))
        else:
            for attr in good[key]['attrib']:
                if attr in excluded_attrs:
                    continue
                good_val = good[key]['attrib'][attr]
                bad_val = bad[key]['attrib'].get(attr)
                if bad_val is None:
                    differences.append((attr, "Attribute missing", good_val, "-"))
                elif good_val != bad_val:
                    differences.append((attr, "Attribute mismatch", good_val, bad_val))
            if good[key]['text'] != bad[key]['text']:
                differences.append(("(text)", "Text mismatch", good[key]['text'], bad[key]['text']))
    for key in bad:
        if key not in good:
            bad_text = f"<{key.split('/')[-1]}>{bad[key]['text']}</{key.split('/')[-1]}>"
            differences.append(("-", "Extra tag", "-", bad_text))
    return differences


def write_case1_csv(differences, output_csv_file):
    with open(output_csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["attribute", "difference type", "wcs value", "microservice value"])
        for row in differences:
            writer.writerow(row)


def process_all_pairs_case1(input_csv_file, output_csv_file):
    excluded_attrs = load_excluded_attributes()
    all_differences = []

    with open(input_csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        print("Detected CSV headers:", reader.fieldnames)

        if "wcs_xml" not in reader.fieldnames or "micro_xml" not in reader.fieldnames:
            raise ValueError("CSV must contain 'wcs_xml' and 'micro_xml' columns.")

        for row in reader:
            good_file = row["wcs_xml"]
            bad_file = row["micro_xml"]
            good_path = os.path.join(XML_FOLDER, good_file)
            bad_path = os.path.join(XML_FOLDER, bad_file)

            print(f" Comparing {good_file} vs {bad_file}...")

            try:
                good_elements = parse_xml_with_lines(good_path)
                bad_elements = parse_xml_with_lines(bad_path)
                diffs = compare_xml_elements(good_elements, bad_elements, excluded_attrs)
                all_differences.extend(diffs)
            except Exception as e:
                print(f"Error comparing {good_file} vs {bad_file}: {e}")

    write_case1_csv(all_differences, output_csv_file)
    print(f"\n ✅ Differences written to: {output_csv_file}")


# === CASE 2: MySQL XML comparison ===
def get_xml_by_order_id(connection, order_id):
    try:
        cursor = connection.cursor()
        query = "SELECT xml_content FROM orders WHERE order_id = %s"
        cursor.execute(query, (order_id,))
        result = cursor.fetchone()
        return result[0] if result else None
    except mysql.connector.Error as e:
        print(f"MySQL error fetching order_id {order_id}: {e}")
        return None


def parse_xml_from_string(xml_string):
    try:
        root = ET.fromstring(xml_string)
        return root, None
    except ET.ParseError as e:
        return None, f"XML ParseError: {e}"


def flatten_elements(root):
    elements = {}
    def recurse(element, path=""):
        new_path = f"{path}/{element.tag}" if path else f"/{element.tag}"
        if new_path not in elements:
            elements[new_path] = {
                "attrib": element.attrib,
                "text": (element.text or "").strip()
            }
        for child in element:
            recurse(child, new_path)
    recurse(root)
    return elements


def compare_xml_elements_dict(good_elements, bad_elements, excluded_attrs):
    differences = []
    for key in good_elements:
        if key not in bad_elements:
            good_text = f"<{key.split('/')[-1]}>{good_elements[key]['text']}</{key.split('/')[-1]}>"
            differences.append(("-", "Tag missing", good_text, "-"))
        else:
            good_attribs = good_elements[key]["attrib"]
            bad_attribs = bad_elements[key]["attrib"]
            for attr in good_attribs:
                if attr in excluded_attrs:
                    continue
                good_val = good_attribs[attr]
                bad_val = bad_attribs.get(attr)
                if bad_val is None:
                    differences.append((attr, "Attribute missing", good_val, "-"))
                elif good_val != bad_val:
                    differences.append((attr, "Attribute mismatch", good_val, bad_val))
            if good_elements[key]["text"] != bad_elements[key]["text"]:
                differences.append(("(text)", "Text mismatch", good_elements[key]["text"], bad_elements[key]["text"]))
    for key in bad_elements:
        if key not in good_elements:
            bad_text = f"<{key.split('/')[-1]}>{bad_elements[key]['text']}</{key.split('/')[-1]}>"
            differences.append(("-", "Extra tag", "-", bad_text))
    return differences


def write_case2_csv(differences, output_csv_file):
    with open(output_csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["attribute", "difference type", "wcs value", "microservice value"])
        for row in differences:
            writer.writerow(row)


def read_order_pairs(csv_filename):
    with open(csv_filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        return [(row['wcs_order_id'], row['micro_order_id']) for row in reader]


def process_all_pairs_case2(output_csv_file):
    excluded_attrs = load_excluded_attributes()
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="KONOHA777",
        database="xml6"
    )
    order_pairs = read_order_pairs("orders_to_compare.csv")
    all_differences = []

    for wcs_id, micro_id in order_pairs:
        wcs_xml = get_xml_by_order_id(connection, wcs_id)
        micro_xml = get_xml_by_order_id(connection, micro_id)

        if not wcs_xml or not micro_xml:
            print(f" Missing XML for pair: {wcs_id}, {micro_id}")
            continue

        wcs_root, wcs_err = parse_xml_from_string(wcs_xml)
        micro_root, micro_err = parse_xml_from_string(micro_xml)

        if wcs_err or micro_err:
            print(f" Parse error for pair: {wcs_id}-{micro_id}")
            continue

        wcs_elements = flatten_elements(wcs_root)
        micro_elements = flatten_elements(micro_root)

        diffs = compare_xml_elements_dict(wcs_elements, micro_elements, excluded_attrs)
        all_differences.extend(diffs)

    connection.close()
    write_case2_csv(all_differences, output_csv_file)
    print(f"\n ✅ Differences written to: {output_csv_file}")


# === MAIN SWITCH ===
def main():
    print("Select option:")
    print("1 - Compare XML files from folder using CSV filenames (Case 1)")
    print("2 - Compare XMLs from MySQL DB using CSV order IDs (Case 2)")
    choice = input("Enter 1 or 2: ").strip()

    if choice == "1":
        input_csv = "input.csv"  # Should have wcs_xml and micro_xml columns
        output_csv = "all_differences_case1.csv"
        process_all_pairs_case1(input_csv, output_csv)
    elif choice == "2":
        output_csv = "all_differences_case2.csv"
        process_all_pairs_case2(output_csv)
    else:
        print("Invalid choice. Exiting.")

if __name__ == "__main__":
    main()
