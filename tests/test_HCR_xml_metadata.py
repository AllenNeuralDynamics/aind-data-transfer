from aind_data_transfer.transformations import converters, file_io
from aind_data_transfer.util import io_utils

from aind_data_transfer.config_loader.imaging_configuration_loader import (
    ImagingJobConfigurationLoader,
)
import pathlib
import json
import datetime
import sys
import argparse
import xml.etree.ElementTree as ET

import unittest

class TestXMLMetadata(unittest.TestCase):
    """Test methods to test the conversion of imaging log to acq json and acq json to xml metadata
    for iSPIM HCR metadata"""

    def setUp(self):
        self.src_data_dir = pathlib.Path(__file__).parent.joinpath('resources', 'imaging', 'iSPIM_test', 'HCR_677594-Christian-ID_2023-10-20_15-10-36')
        self.dest_data_dir = "HCR_677594_2023-10-20_15-10-36"

    
    def read_xml(self, path: str) -> ET.ElementTree:
        """Read xml file into ElementTree, return string of XML"""
        xml_file = ET.parse(path)
        
        #convert to str
        xml_str = ET.tostring(xml_file.getroot(), encoding='unicode', method='xml')
        return  ET.canonicalize(xml_str, strip_text=True)

    def test_xml_write(self):
        data_src_dir, dest_data_dir = self.src_data_dir, self.dest_data_dir
        
        log_file = data_src_dir.joinpath('imaging_log.log')
        acq_file = data_src_dir.joinpath('dispim_acquisition.json')


        toml_dict = file_io.read_toml(data_src_dir.joinpath('config.toml'))
        

        # read log file into dict
        if not log_file.exists():
            log_dict = file_io.read_imaging_log(log_file)
            log_dict['data_src_dir'] = (data_src_dir.as_posix())
            log_dict['config_toml'] = toml_dict
        
        else: 
            log_dict = {'imaging_log_file': None} #set this to none to read schema_log


        if acq_file.exists():
            acq_json = converters.read_dispim_aquisition(acq_file)
            

        #if any of the values of log_dict are None, then get it from schema_log
        elif any(v is None for v in log_dict.values()):
            print('Some values in imaging_log.log are None. Reading schema_log.log')
            log_file =  data_src_dir.joinpath('schema_log.log')
            log_dict = {}
            log_dict = file_io.read_schema_log_file(log_file)
            log_dict['data_src_dir'] = (data_src_dir.as_posix())
            log_dict['config_toml'] = toml_dict
            print('Finished reading schema_log.log')
            try:
                acq_json = converters.schema_log_to_acq_json(log_dict)
                print('Finished converting schema_log.log to acq json')
            except Exception as e:
                print(f"Failed to convert schema_log.log to acq json: {e}") 

        else:
            # convert imaging_log to acq json
            try:
                acq_json = converters.log_to_acq_json(log_dict)
            except Exception as e:
                print(f"Failed to convert imaging_log.log to acq json: {e}")

        # convert to acq json
        acq_json_path = data_src_dir.joinpath('acquisition.json')


        try:
            file_io.write_acq_json(acq_json, acq_json_path)
            print('Finished writing acq json')
        except Exception as e:
            print(f"Failed to write acquisition.json: {e}")

        # convert acq json to xml
        is_zarr = True
        condition = "channel=='405'"
        acq_xml = converters.acq_json_to_xml(acq_json, log_dict, str(dest_data_dir) + f'/SPIM.ome.zarr', is_zarr, condition)  # needs relative path to zarr file (as seen by code ocean)

        # write xml to file
        xml_file_path = data_src_dir.joinpath('Test.xml')  #
        file_io.write_xml(acq_xml, xml_file_path)

        #read test xml
        test_xml = self.read_xml(xml_file_path)
        

        #read reference xml file to be compared against
        ref_xml_file_path = data_src_dir.joinpath('Reference.xml')
        ref_xml = self.read_xml(ref_xml_file_path)


        #compare test xml to reference xml
        self.assertEqual(test_xml, ref_xml)

        #delete test xml file
        xml_file_path.unlink()
        


if __name__ == "__main__":
    unittest.main()
