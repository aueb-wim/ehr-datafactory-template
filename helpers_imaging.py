
import csv
import os

def split_subjectcode(inputpath, outputpath):
    with open(inputpath, 'r') as fp_in, open(outputpath, 'w') as fp_out:
        reader = csv.reader(fp_in, delimiter=',')
        headers = next(reader)

        writer = csv.writer(fp_out, delimiter=',')
        writer.writerow(['PATIENT_ID', 'VISIT_ID'] + headers)

        for row in reader:
            patient_id, study_id = row[0].split('_')
            writer.writerow([patient_id, study_id] + row)
