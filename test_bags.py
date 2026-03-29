"""Test script for bags processing"""
from processing.dsp_core import generate_bags_messages

# Test with BOM and quoted headers exactly like the actual file
test_csv = b'''\xef\xbb\xbf"Country","Delivery Station","DSP","Route Code","Route ID","Transporter_id","Year_week","Date","Store Name","Bag","Unrecovered"
"UK","DNR1","ATAG","CA_A141","7422563-141","A32FHWW7TIRT29","2026-13","2026-03-22 00:00:00","","EU_OB-CY-0159_NVY",1
"UK","DNR1","ATAG","CA_A141","7422563-141","A32FHWW7TIRT29","2026-13","2026-03-22 00:00:00","","EU_OB-CY-0160_NVY",1
"UK","DNR1","DELL","CA_A100","7424158-100","A37VY76CWSNBK4","2026-13","2026-03-23 00:00:00","","EU_OB-CT-4008_YLO",1
"UK","DNR1","DELL","CA_A100","7425835-100","A1G2FC5JSV0J92","2026-13","2026-03-24 00:00:00","","EU_OB-CM-7462_BLK",1
'''

try:
    result = generate_bags_messages(test_csv)
    print(f'Success! Generated {len(result)} messages')
    for dsp in result:
        print(f'  - {dsp}: {len(result[dsp])} chars')
except Exception as e:
    print(f'ERROR: {type(e).__name__}: {e}')
