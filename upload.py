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
        "http://192.168.240.19:9000/fireuser/bf2f59a8-aed5-4ee5-87e2-e84ea0bcd012/temp20M-10.dat?uploadId=9aba9b21-facc-4787-83dc-08b158e2538f&partNumber=1&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=storage_access_key%2F20250122%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20250122T170024Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=5caed7c1ef1b932f467af2764db32c90f79ef2b9b925788f649d7b4cf76a0aa9",
        "http://192.168.240.19:9000/fireuser/bf2f59a8-aed5-4ee5-87e2-e84ea0bcd012/temp20M-10.dat?uploadId=9aba9b21-facc-4787-83dc-08b158e2538f&partNumber=2&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=storage_access_key%2F20250122%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20250122T170024Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=b90e72de5c838fd5b811dda0cb5ae94f9787671f26f5af9212bbfc0850180d8e"
    ],
    "completeUploadUrl": "http://192.168.240.19:9000/fireuser/bf2f59a8-aed5-4ee5-87e2-e84ea0bcd012/temp20M-10.dat?uploadId=9aba9b21-facc-4787-83dc-08b158e2538f&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=storage_access_key%2F20250122%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20250122T170024Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=ca102e9cdf68fd3950c102ba5ec19849be1a5ac0aa3d8b781167a296e90b0482",
    "maxPartSize": 10485760,
    "transferJob": {
        "jobId": 8,
        "system": "cluster-slurm-api",
        "workingDirectory": "/home/fireuser",
        "logs": {
            "outputLog": "/home/fireuser/.f7t_file_handling_job_282791a0-1cf3-48db-98b9-de8b48295434.log",
            "errorLog": "/home/fireuser/.f7t_file_handling_job_error_282791a0-1cf3-48db-98b9-de8b48295434.log"
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
