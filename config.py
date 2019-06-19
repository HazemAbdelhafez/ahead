from collections import OrderedDict
from node_info import Node


# Variables
CORAL2_APPS = ['qmcpack', 'lammps', 'minife', 'pennant', 'laghos']


reference_node = Node(name='Titan', app_traces_dir='data/%s_Data/Applications_Traces/',
                      mb_traces_dir="data/%s_Data/Bandwidth_Benchmarking/", apps=CORAL2_APPS)

target_node = Node(name='Summit', app_traces_dir='data/%s_Data/Applications_Traces/',
                   mb_traces_dir="data/%s_Data/Bandwidth_Benchmarking/", apps=CORAL2_APPS)

REFERENCE_NODE = reference_node.get_name()
TARGET_NODE = target_node.get_name()

# File Names
BASE_FILE_NAME = "profiler-bandwidth-output"

# CUPTI configurations

directions_codes_dictionary = OrderedDict()  # Memory copy id to string map
HTOD_PAGED = 101
DTOH_PAGED = 102
DTOD = 103
HTOD_PINNED = 104
DTOH_PINNED = 105
HTOH_PINNED = 106

MEM_KIND = 'mem_kind'
MEM_DEVICE_KIND = 'device_kind'

cupti_map = OrderedDict()
cupti_map[MEM_KIND] = OrderedDict()
cupti_map[MEM_KIND][0] = 'Unknown'
cupti_map[MEM_KIND][1] = 'Paged'
cupti_map[MEM_KIND][2] = 'Pinned'
cupti_map[MEM_KIND][3] = 'Device'
cupti_map[MEM_KIND][4] = 'Array'
cupti_map[MEM_KIND][5] = 'Managed'
cupti_map[MEM_KIND][6] = 'Device_static'
cupti_map[MEM_KIND][7] = 'Managed_static'

cupti_map[MEM_DEVICE_KIND] = OrderedDict()
cupti_map[MEM_DEVICE_KIND][0] = 'Unknown'
cupti_map[MEM_DEVICE_KIND][1] = 'HTOD'
cupti_map[MEM_DEVICE_KIND][2] = 'DTOH'
cupti_map[MEM_DEVICE_KIND][3] = 'HTOA'
cupti_map[MEM_DEVICE_KIND][4] = 'ATOH'
cupti_map[MEM_DEVICE_KIND][5] = 'ATOA'
cupti_map[MEM_DEVICE_KIND][6] = 'ATOD'
cupti_map[MEM_DEVICE_KIND][7] = 'DTOA'
cupti_map[MEM_DEVICE_KIND][8] = 'DTOD'
cupti_map[MEM_DEVICE_KIND][9] = 'HTOH'
cupti_map[MEM_DEVICE_KIND][10] = 'PTOP'

directions_codes_dictionary[HTOD_PAGED] = [113, 314]
directions_codes_dictionary[DTOH_PAGED] = [231]
directions_codes_dictionary[DTOD] = [833]
directions_codes_dictionary[HTOD_PINNED] = [123]
directions_codes_dictionary[DTOH_PINNED] = [232]
directions_codes_dictionary[HTOH_PINNED] = [922]
