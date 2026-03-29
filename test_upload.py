"""Test script to simulate Flask file upload handling"""
import io
from werkzeug.datastructures import FileStorage

# Simulate what Flask receives
test_csv_content = b'''\xef\xbb\xbf"Country","Delivery Station","DSP","Route Code","Route ID","Transporter_id","Year_week","Date","Store Name","Bag","Unrecovered"
"UK","DNR1","ATAG","CA_A141","7422563-141","A32FHWW7TIRT29","2026-13","2026-03-22 00:00:00","","EU_OB-CY-0159_NVY",1
"UK","DNR1","ATAG","CA_A141","7422563-141","A32FHWW7TIRT29","2026-13","2026-03-22 00:00:00","","EU_OB-CY-0160_NVY",1
'''

# Create a FileStorage object like Flask would
csv_file = FileStorage(
    stream=io.BytesIO(test_csv_content),
    filename='test.csv',
    content_type='text/csv'
)

# Simulate what app.py does
print(f'Filename: {csv_file.filename}')
print(f'Filename type: {type(csv_file.filename)}')
print(f'Has filename: {bool(csv_file.filename)}')

# Read the file
file_bytes = csv_file.read()
print(f'File bytes length: {len(file_bytes)}')
print(f'File bytes type: {type(file_bytes)}')
print(f'First 50 bytes: {file_bytes[:50]}')

# Now call the processor
from processing.dsp_core import generate_bags_messages
result = generate_bags_messages(file_bytes)
print(f'Success! Generated {len(result)} messages')
