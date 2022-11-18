import numpy as np
import logging
from pathlib import Path
from packaging.version import parse
import xml.etree.ElementTree as ET

# geometry properties for NP-opto
X_PITCH = 48
X_OFFSET = 11
Y_PITCH = 20
NUMEL_IN_COL = 192


def get_standard_np_opto_electrode_positions():
    npopto_electrode_xpos_arr = [X_OFFSET, X_OFFSET + X_PITCH] * NUMEL_IN_COL
    npopto_electrode_ypos_arr = np.concatenate([i * np.array([Y_PITCH, Y_PITCH])
                                                for i in range(NUMEL_IN_COL)])
    npopto_electrode_xpos = {}
    npopto_electrode_ypos = {}
    for ch, (xpos, ypos) in enumerate(zip(npopto_electrode_xpos_arr,
                                          npopto_electrode_ypos_arr)):
        npopto_electrode_xpos[f"CH{ch}"] = str(xpos)
        npopto_electrode_ypos[f"CH{ch}"] = str(ypos)
    return npopto_electrode_xpos, npopto_electrode_ypos


def correct_np_opto_electrode_locations(
    input_dir
):
    # find settings files
    input_dir = Path(input_dir)
    settings_files = list(input_dir.glob('**/*.xml'))

    npopto_electrode_xpos, npopto_electrode_ypos = get_standard_np_opto_electrode_positions()

    for settings_file in settings_files:
        # parse xml
        tree = ET.parse(str(settings_file))
        root = tree.getroot()

        signal_chain = root.find("SIGNALCHAIN")
        neuropix_pxi = None
        for processor in signal_chain:
            if "PROCESSOR" == processor.tag:
                name = processor.attrib["name"]
                if "Neuropix-PXI" in name:
                    neuropix_pxi = processor
                    break

        if neuropix_pxi is None:
            return
        neuropix_pxi_version = parse(neuropix_pxi.attrib["libraryVersion"])
        if neuropix_pxi_version > parse("0.4.0"):
            # no correction needed for this plugin version
            return

        editor = neuropix_pxi.find("EDITOR")
        np_probes = editor.findall("NP_PROBE")
        
        needs_correction = False
        for np_probe in np_probes:
            if "OPTO" in np_probe.attrib["headstage_part_number"]:
                logging.info("Found NP-OPTO!")
                needs_correction = True

                # update channel locations                
                electrode_xpos = np_probe.find("ELECTRODE_XPOS")
                electrode_ypos = np_probe.find("ELECTRODE_YPOS")
                electrode_xpos.attrib = npopto_electrode_xpos
                electrode_ypos.attrib = npopto_electrode_ypos

        settings_file_path = str(settings_file)
        if needs_correction:
            wrong_settings_file = settings_file.parent / "settings.xml.wrong"
            logging.info(f"Renaming wrong NP-OPTO settings file as "
                         f"{wrong_settings_file}")
            settings_file.rename(wrong_settings_file)
            logging.info(f"Saving correct NP-OPTO settings file as "
                         f"{settings_file_path}")
            tree.write(settings_file_path)
        else:
            logging.info(f"No NP-OPTO probes found in {settings_file_path}")
