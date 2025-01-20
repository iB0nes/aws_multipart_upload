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
    "partsUploadUrlList": [
        "http://192.168.240.19:9000/fireuser/13a66c6e-39d0-4b77-a9bb-4d3670b7983a/temp20M.dat?uploadId=de43a4c3-37d4-495f-9cee-315ed18f0451&partNumber=1&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=storage_access_key%2F20250117%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20250117T101825Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=68223de9341f52312c77050d4b2191d71389c5f8ae84cb566d0526b13f7ceee5",
        "http://192.168.240.19:9000/fireuser/13a66c6e-39d0-4b77-a9bb-4d3670b7983a/temp20M.dat?uploadId=de43a4c3-37d4-495f-9cee-315ed18f0451&partNumber=2&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=storage_access_key%2F20250117%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20250117T101825Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=d33892893739f65683b685ba5b91c05adf187298477d27b9af354f7f2451f460"
    ],
    "completeUploadUrl": "http://192.168.240.19:9000/fireuser/13a66c6e-39d0-4b77-a9bb-4d3670b7983a/temp20M.dat?uploadId=de43a4c3-37d4-495f-9cee-315ed18f0451&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=storage_access_key%2F20250117%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20250117T101825Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=306a5873edeead13c86ac4742c977f810f2b64cd7ef5a168140ad4e9f12aee00",
    "maxPartSize": 10485760,
    "transferJob": {
        "jobId": 5,
        "system": "cluster-slurm-api",
        "workingDirectory": "/home/fireuser",
        "logs": {
            "outputLog": "/home/fireuser/.f7t_file_handling_job_5a1be2d0-e8b1-450c-8c5e-3330c13c5a9d.log",
            "errorLog": "/home/fireuser/.f7t_file_handling_job_error_5a1be2d0-e8b1-450c-8c5e-3330c13c5a9d.log"
        }
    }
}

file = "17M.dat"
full_size = os.stat(file).st_size
etags = []

print(full_size)

with open(file, 'rb') as f:
    for i in range(1, len(json_response["partsUploadUrlList"]) + 1):
        url = json_response["partsUploadUrlList"][i-1]
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
