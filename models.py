import math
BITS_PER_BYTE = 8       # Just to remember why we divide by 8 in the following equations


# Base models for memories and links

class MemoryModel:
    _bus_width = 128             # Bits
    _mem_frequency = 2505.0        # Mhz
    transfers_per_cycle = 2              # 2 transfers per cycle (at the high and low edges of the clock)

    def __init__(self, bus_width=128, mem_freq=2505.0, transfers_per_cycle=2, bidirectional_factor=2):
        self._bus_width = bus_width
        self._mem_frequency = mem_freq
        self._transfers_per_cycle = transfers_per_cycle
        self._bidirectional_factor = bidirectional_factor

    def get_max_unidirectional_bandwidth(self):
        _max_bw = (self._bus_width * self._mem_frequency) / BITS_PER_BYTE
        _max_bw /= 1e3                  # Bandwidth in GB/sec
        return _max_bw

    def get_effective_unidirectional_bandwidth(self, _data_size):
        pass

    def get_bidirectional_factor(self):
        return self._bidirectional_factor


class LinkModel:

    # Parameters
    _latency = 0.0
    _bw = 1.0

    def __init__(self, latency=0.0, bw=1.0):
        self._bw = bw
        self._latency = latency

    def get_number_of_transferred_bytes(self, _size, _mode='r'):
        pass

    def calculate_number_of_transferred_bytes(self, input_sizes_array, mode='r'):
        pass

    def calculate_link_max_bw(self):
        pass

    def calculate_link_theoretical_bw(self):
        pass

    def apply_model(self, input_sizes_array, software_overhead, mode, no_overhead=False):
        pass

    def get_effective_bandwidth(self, _size, _mode):
        pass

    def set_latency(self, latency=0.0):
        self._latency = latency


# Specific models of the systems' components considered in our study
class GPUMemoryModel(MemoryModel):
    def __init__(self, bus_width=128, mem_freq=2505, ecc_impact=0.0, transfers_per_cycle=2):
        MemoryModel.__init__(self,
                             bus_width=bus_width, mem_freq=mem_freq * (1-ecc_impact/100),
                             transfers_per_cycle=transfers_per_cycle, bidirectional_factor=2)


class DRAMModel(MemoryModel):
    _latency = 0.0
    _memory_banks_per_cpu = 1
    _num_cpus = 1
    _memory_module_size = 0.0
    _ecc_overhead = 0.0

    def __init__(self, latency=0.0, mem_freq=0.0, mem_banks_per_cpu=1,
                 mem_module_size=0.0,
                 bus_width=0.0, num_cpus=2, ecc_overhead=0.0):
        MemoryModel.__init__(self,
                             bus_width=bus_width, mem_freq=mem_freq, transfers_per_cycle=2, bidirectional_factor=2)
        self._latency = latency
        self._memory_banks_per_cpu = mem_banks_per_cpu
        self._memory_module_size = mem_module_size
        self._num_cpus = num_cpus
        self._ecc_overhead = ecc_overhead

    def get_max_unidirectional_bandwidth(self):
        _max_bw = (self._bus_width * self._mem_frequency * self._transfers_per_cycle * (1-self._ecc_overhead/100)
                   * self._memory_banks_per_cpu * self._num_cpus) / \
                  BITS_PER_BYTE
        _max_bw /= 1e3  # Bandwidth in GB/sec
        return _max_bw

    def get_effective_unidirectional_bandwidth(self, _data_size):
        _max_bw = self.get_max_unidirectional_bandwidth()  # GB/sec
        _max_bw /= self._num_cpus
        return _max_bw


class PCILinkModel(LinkModel):
    # Additional Parameters
    mps = 256
    mrrs = 512
    mw_o = 30
    mr_o = 30
    cpd_o = 20
    rcb = 64
    num_lanes = 8                   #
    max_lane_bw = 8                 # GT/sec
    encoding_efficiency = 98.5      # Encoding efficiency percentage (e.g. 128B/130B for PCIe Gen 3)
    data_link_overhead = 0.0         # Data link overhead is in the range of 10% according to literature survey

    def __init__(self, latency=0.0, mps=128, mrrs=512, mw_o=30, mr_o=30, cpd_o=20,
                 rcb=64,
                 num_lanes=8,
                 max_lane_bw=8,
                 encoding_efficiency=98.5,
                 data_link_overhead=0.0):
        LinkModel.__init__(self, latency=latency, bw=1.0)
        self.mps = mps
        self.mrrs = mrrs
        self.mw_o = mw_o
        self.mr_o = mr_o
        self.cpd_o = cpd_o
        self.rcb = rcb
        self.num_lanes = num_lanes
        self.max_lane_bw = max_lane_bw
        self.encoding_efficiency = encoding_efficiency
        self.data_link_overhead = data_link_overhead

    def get_number_of_transferred_bytes(self, _size, _mode='r'):
        if _mode == 'r':
            rx = math.ceil(_size / min(self.rcb, self.mps)) * self.cpd_o + _size  # Received data traffic size
            _sz = rx + min(self.mrrs, _size) + self.mr_o
        else:
            _sz = math.ceil(_size / self.mps) * self.mw_o + _size
        return _sz

    def calculate_number_of_transferred_bytes(self, input_sizes_array, mode='r'):
        transferred_bytes_array = []
        for n in input_sizes_array:
            sz = self.get_number_of_transferred_bytes(n, mode)
            transferred_bytes_array.append(sz)
        return transferred_bytes_array

    def calculate_link_max_bw(self):
        link_max_bw = (self.num_lanes * self.max_lane_bw) / BITS_PER_BYTE  # GBytes/sec
        link_bw = link_max_bw * (self.encoding_efficiency/100) * (1-self.data_link_overhead/100)
        return link_bw

    def calculate_link_theoretical_bw(self):
        link_max_bw = (self.num_lanes * self.max_lane_bw) / BITS_PER_BYTE  # GBytes/sec
        return link_max_bw

    def apply_model(self, input_sizes_array, single_byte_transfer_time, mode, no_overhead=False):
        num_of_bytes = self.calculate_number_of_transferred_bytes(input_sizes_array=input_sizes_array, mode=mode)
        max_bw = self.calculate_link_max_bw()
        _transfer_time = []
        for _size in num_of_bytes:
            if no_overhead:
                _transfer_time.append(_size/max_bw)
            else:
                _transfer_time.append(single_byte_transfer_time + (_size-1)/max_bw)
        return _transfer_time     # In (ns)

    def get_effective_bandwidth(self, _size, _mode):
        num_bytes = self.get_number_of_transferred_bytes(_size, _mode)
        effective_bw = _size / (num_bytes / self.calculate_link_max_bw())
        return effective_bw


class NvLinkModel(LinkModel):
    # Additional Parameters
    _flit_size = 16                                                     # Bytes
    _max_payload_size_in_flits = 16                                     # Flits
    _max_num_flits_per_packet = 18
    _num_lanes_per_link = 8
    _transactions_rate_per_lane = 25    # GT/s
    _num_links_per_chip = 6
    __num_bricks_per_interface = 2

    def __init__(self, latency=0.0):
        LinkModel.__init__(self, latency=latency)

    def get_number_of_transferred_bytes(self, _size, _mode='r'):
        _sz = 0
        if _mode == 'r' or _mode == 'w':        # We assume that the read and write overhead is the same for NvLink
            # according to what we read so far
            tx = self._flit_size * 1            # A read request is sent in a single header for 256 Bytes,
            # we assume that the reads are pipelined so that the receiver always has available requests to serve, thus
            # the penalty of the read requests is hidden and only the delay for the first request is incorporated.
            max_payload_in_bytes = self._max_payload_size_in_flits * self._flit_size
            rx = math.ceil(_size/max_payload_in_bytes) * self._flit_size + _size
            _sz = tx + rx
        elif _mode == 'w':
            max_payload_in_bytes = self._max_payload_size_in_flits * self._flit_size
            tx = math.ceil(_size/max_payload_in_bytes) * self._flit_size + _size
            _sz = tx
        else:
            print("Invalid mode")
            exit(-1)
        return _sz

    def calculate_number_of_transferred_bytes(self, input_sizes_array, mode='r'):
        transferred_bytes_array = []
        for n in input_sizes_array:
            sz = self.get_number_of_transferred_bytes(n, mode)
            transferred_bytes_array.append(sz)
        return transferred_bytes_array

    def calculate_link_max_bw(self):
        link_max_unidirectional_bw = (self._num_lanes_per_link *
                                      self._transactions_rate_per_lane) / BITS_PER_BYTE     # GB/s
        link_bw = link_max_unidirectional_bw * self.__num_bricks_per_interface
        return link_bw

    def calculate_link_theoretical_bw(self):
        return self.calculate_link_max_bw()

    def apply_model(self, input_sizes_array, single_byte_transfer_time, mode, no_overhead=False):
        num_of_bytes = self.calculate_number_of_transferred_bytes(input_sizes_array=input_sizes_array, mode=mode)
        max_bw = self.calculate_link_max_bw()
        _transfer_time = []
        for _size in num_of_bytes:
            if no_overhead:
                _transfer_time.append(_size/max_bw)
            else:
                _transfer_time.append(single_byte_transfer_time + (_size-1)/max_bw)
        return _transfer_time     # In (ns)

    def get_effective_bandwidth(self, _size, _mode):
        num_bytes = self.get_number_of_transferred_bytes(_size, _mode)
        effective_bw = _size / (num_bytes / self.calculate_link_max_bw())
        return effective_bw
