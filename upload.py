import os
import requests
from xml.etree import ElementTree

def get_checksum_xml(part_dictionary, root_node_tag):
    root = ElementTree.Element(root_node_tag, {'xmlns': "http://s3.amazonaws.com/doc/2006-03-01/"})
    for p in part_dictionary:
         part_element = ElementTree.SubElement(root, 'Part')
         part_etag = ElementTree.SubElement(part_element, 'ETag')
         part_etag.text = p['ETag']
         part_number = ElementTree.SubElement(part_element, 'PartNumber')
         part_number.text = str(p['PartNumber'])
    return ElementTree.tostring(root, encoding='utf-8', xml_declaration=True, method='xml').decode('utf-8')


json_response = {
    "partsUploadUrls": [
        "http://localhost:9000/fireuser/e6758864-08ec-49ba-a7ab-137f0774bd82/temp20M-12.dat?uploadId=afca164f-9b20-4d3f-9661-d301b2ee0a8c&partNumber=1&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=storage_access_key%2F20250203%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20250203T151129Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=8422f9d162c39e378f459c62097b7889e46c776bb2c860a135606638cf48398e",
        "http://localhost:9000/fireuser/e6758864-08ec-49ba-a7ab-137f0774bd82/temp20M-12.dat?uploadId=afca164f-9b20-4d3f-9661-d301b2ee0a8c&partNumber=2&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=storage_access_key%2F20250203%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20250203T151129Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=3eedf1de43ea738366a2531550feb33d8419330104e2265a0b733fa81d3dc792"
    ],
    "completeUploadUrl": "http://localhost:9000/fireuser/e6758864-08ec-49ba-a7ab-137f0774bd82/temp20M-12.dat?uploadId=afca164f-9b20-4d3f-9661-d301b2ee0a8c&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=storage_access_key%2F20250203%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20250203T151129Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=b1d30f51cad5656e8c9963d013047ad1f9af27d1c2eda44e51300c4f470c0174",
    "maxPartSize": 10485760,
    "transferJob": {
        "jobId": 16,
        "system": "cluster-slurm-api",
        "workingDirectory": "/home/fireuser",
        "logs": {
            "outputLog": "/home/fireuser/.f7t_file_handling_job_a1196def-ce53-423d-a5f2-b2f89756e3d3.log",
            "errorLog": "/home/fireuser/.f7t_file_handling_job_error_a1196def-ce53-423d-a5f2-b2f89756e3d3.log"
        }
    }
}

file = "17M.dat"
full_size = os.stat(file).st_size
etags = []

print(full_size)

with open(file, 'rb') as f:
    for i in range(1, len(json_response["partsUploadUrls"]) + 1):
        url = json_response["partsUploadUrls"][i-1]
        data_chunk = f.read(json_response["maxPartSize"])
        print(f"{i} {len(data_chunk)} {url}")

        r = requests.put(url, data=data_chunk)
        e_tag = r.headers['ETag']
        print(r.status_code)
        if not r.ok:
            print(r.text)
            raise Exception(f"UploadPart failed with code {r.status_code}")
        etags.append({'PartNumber': i, 'ETag': e_tag})

print(etags)
checksum = get_checksum_xml(etags, 'CompleteMultipartUpload')
print(checksum)
r = requests.post(json_response["completeUploadUrl"], data=checksum)
print(r)
