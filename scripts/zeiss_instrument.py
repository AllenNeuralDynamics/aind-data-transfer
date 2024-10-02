""" example AIND Zeiss Lightsheet 7 instrument """

import datetime

from aind_data_schema_models.organizations import Organization

from aind_data_schema.components.devices import (
    AdditionalImagingDevice,
    Detector,
    Filter,
    Laser,
    MotorizedStage,
    Objective,
    OpticalTable,
    ScanningStage,
    DetectorType,
    DataInterface,
)
from aind_data_schema.core.instrument import Com, Instrument

inst = Instrument(
    instrument_id="420_Zeiss_Lightsheet7_1",
    modification_date=datetime.date(2024, 2, 28),
    instrument_type="diSPIM",
    manufacturer=Organization.CARL_ZEISS,
    objectives=[
        Objective(
            name="Detection Objective",
            numerical_aperture=1,
            magnification=20,
            immersion="water",
            manufacturer=Organization.CARL_ZEISS,
            model="W Plan-Apochromat 20x/1.0 Corr_4909000329",
            notes="",
        ) #there are also a 1.4x illumination zoom optic, and a variable zoom optic compontents
    ],
    detectors=[
        Detector(
            detector_type=DetectorType.CAMERA,
            data_interface=DataInterface.USB,
            name="Detector:0",
            cooling="Water",
            manufacturer=Organization.OTHER, #Excelitas Technologies
            model="pco.edge 4.2",
            serial_number="61008897",
        ),
        Detector(
            detector_type=DetectorType.CAMERA,
            data_interface=DataInterface.USB,
            name="Detector:1",
            cooling="Water",
            manufacturer=Organization.OTHER, #Excelitas Technologies
            model="pco.edge 4.2",
            serial_number="61008901",
        ),
    ],
    light_sources=[
        Laser(
            name="405_50",
            device_type="Laser",
            coupling="Single-mode fiber",
            wavelength=405,
            maximum_power=50,
            serial_number="",
            manufacturer=Organization.OTHER, #"Lasos",
        ),
        Laser(
            name="488_50",
            device_type="Laser",
            coupling="Single-mode fiber",
            wavelength=488,
            maximum_power=50,
            serial_number="",
            manufacturer=Organization.OTHER, #"Lasos",,
        ),
        Laser(
            name="514_20",
            device_type="Laser",
            coupling="Single-mode fiber",
            wavelength=514,
            maximum_power=20,
            serial_number="",
            manufacturer=Organization.OTHER, #"Lasos",,
        ),
        Laser(
            name="561_50",
            device_type="Laser",
            coupling="Single-mode fiber",
            wavelength=561,
            maximum_power=50,
            serial_number="",
            manufacturer=Organization.OTHER, #"Lasos",,
        ),
        Laser(
            name="594_30",
            device_type="Laser",
            coupling="Single-mode fiber",
            wavelength=594,
            maximum_power=30,
            serial_number="",
            manufacturer=Organization.OTHER, #"Lasos",,
        ),
        Laser(
            name="638_75",
            device_type="Laser",
            coupling="Single-mode fiber",
            wavelength=638,
            maximum_power=75,
            serial_number="",
            manufacturer=Organization.OTHER, #"Lasos",,
        ),
    ],
    fluorescence_filters=[
        Filter(
            name="LBF 405/488/561/640", #FW 1
            filter_type="Notch",
            manufacturer=Organization.OTHER, #Carl Zeiss AFAIK
            model="",
            filter_wheel_index=0,
            serial_number="Unknown-0",
        ),
        Filter(
            name="LBF 488/594",#FW 1
            filter_type="Notch",
            manufacturer=Organization.OTHER,
            model="",
            filter_wheel_index=1,
            serial_number="Unknown-0",
        ),
        Filter(
            name="LBF 445/515/638", #FW 1
            filter_type="Notch",
            manufacturer=Organization.OTHER,
            model="",
            filter_wheel_index=2,
            serial_number="Unknown-0",
        ),
        Filter(
            name="SBS LP 490", #FW 1
            filter_type="Dichroic",
            manufacturer=Organization.OTHER,
            model="",
            filter_wheel_index=3,
            serial_number="Unknown-0",
        ),
        Filter(
            name="SBS LP 490", #FW 1
            filter_type="Dichroic",
            manufacturer=Organization.OTHER,
            model="",
            filter_wheel_index=4,
            serial_number="Unknown-0",
        ),
        Filter(
            name="SBS LP 560", #FW 1
            filter_type="Dichroic",
            manufacturer=Organization.OTHER,
            model="",
            filter_wheel_index=5,
            serial_number="Unknown-0",
        ),
        Filter(
            name="SBS LP 580", #FW 1
            filter_type="Dichroic",
            manufacturer=Organization.OTHER,
            model="",
            filter_wheel_index=6,
            serial_number="Unknown-0",
        ),
        #FW 2 Below
        
        Filter(
            name="BP 420-470",
            filter_type="Band pass",
            manufacturer=Organization.OTHER,
            model="",
            filter_wheel_index=7,
            serial_number="Unknown-0",
        ),
        
        Filter(
            name="BP 505-545",
            filter_type="Band pass",
            manufacturer=Organization.OTHER,
            model="",
            filter_wheel_index=8,
            serial_number="Unknown-0",
        ),
        Filter(
            name="BP 525-565",
            filter_type="Band pass",
            manufacturer=Organization.OTHER,
            model="",
            filter_wheel_index=9,
            serial_number="Unknown-0",
        ),

        Filter(
            name="BP 575-615",
            filter_type="Band pass",
            manufacturer=Organization.OTHER,
            model="",
            filter_wheel_index=10,
            serial_number="Unknown-0",
        ),

        Filter(
            name="BP 605-700",
            filter_type="Band pass",
            manufacturer=Organization.OTHER,
            model="",
            filter_wheel_index=11,
            serial_number="Unknown-0",
        ),
        Filter(
            name="LP 585",
            filter_type="Long pass",
            manufacturer=Organization.OTHER,
            model="",
            filter_wheel_index=12,
            serial_number="Unknown-0",
        ),
    ],
    motorized_stages=[
        MotorizedStage(
            model="LS-100",
            manufacturer=Organization.ASI,
            serial_number="Unknown-1",
            travel=100,
            name="Focus stage",
        ),
        MotorizedStage(
            model="L12-20F-4",
            manufacturer=Organization.ASI,
            serial_number="Unknown-5",
            travel=41,
            name="Cylindrical lens #1",
        ),
    ],
    scanning_stages=[
        ScanningStage(
            model="LS-50",
            manufacturer=Organization.ASI,
            serial_number="Unknown-2",
            stage_axis_direction="Detection axis",
            stage_axis_name="Z",
            travel=50,
            name="Sample stage Z",
        ),
        ScanningStage(
            model="LS-50",
            manufacturer=Organization.ASI,
            serial_number="Unknown-3",
            stage_axis_direction="Illumination axis",
            stage_axis_name="X",
            travel=50,
            name="Sample stage X",
        ),
        ScanningStage(
            model="LS-50",
            manufacturer=Organization.ASI,
            serial_number="Unknown-4",
            stage_axis_direction="Perpendicular axis",
            stage_axis_name="Y",
            travel=50,
            name="Sample stage Y",
        ),
    ],
    optical_tables=[
        OpticalTable(
            name="Main optical table",
            length=36,
            width=30,
            vibration_control=True,
            model="m-VIS 30x36",
            manufacturer=Organization.MKS_NEWPORT,
            serial_number="Unknown",
        )
    ],
    com_ports=[
        Com(hardware_name="Laser Launch", com_port="COM4"),
        Com(
            hardware_name="ASI Tiger",
            com_port="COM3",
        ),
        Com(
            hardware_name="MightyZap",
            com_port="COM9",
        ),
    ],
    humidity_control=False,
    temperature_control=False,
)

serialized = inst.model_dump_json()
deserialized = Instrument.model_validate_json(serialized)
deserialized.write_standard_file(prefix="aind_zeiss_LS7_1")