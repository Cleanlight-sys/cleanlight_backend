# arithmeticcoding.py
# MIT License

class ArithmeticCoderBase:
    def __init__(self, num_bits):
        self.num_bits = num_bits
        self.full_range = 1 << self.num_bits
        self.half_range = self.full_range >> 1
        self.quarter_range = self.half_range >> 1
        self.minimum_range = self.quarter_range + 2
        self.maximum_total = self.minimum_range
        self.state_mask = self.full_range - 1

        self.low = 0
        self.high = self.state_mask

        self.underflow = 0

class ArithmeticEncoder(ArithmeticCoderBase):
    def __init__(self, num_bits, out):
        super().__init__(num_bits)
        self.output = out
        self.pending_bits = 0
        self.current_byte = 0
        self.bits_filled = 0

    def write(self, freq, symbol):
        total = freq.get_total()
        low_count = freq.get_low(symbol)
        high_count = freq.get_high(symbol)
        range_ = self.high - self.low + 1
        self.high = self.low + (range_ * high_count // total) - 1
        self.low = self.low + (range_ * low_count // total)

        while True:
            if self.high < self.half_range:
                self._write_bit(0)
            elif self.low >= self.half_range:
                self._write_bit(1)
                self.low -= self.half_range
                self.high -= self.half_range
            elif self.low >= self.quarter_range and self.high < self.quarter_range * 3:
                self.underflow += 1
                self.low -= self.quarter_range
                self.high -= self.quarter_range
            else:
                break
            self.low = self.low << 1 & self.state_mask
            self.high = (self.high << 1 & self.state_mask) | 1

    def _write_bit(self, bit):
        self.output.write(bytes([bit]))
        for _ in range(self.underflow):
            self.output.write(bytes([bit ^ 1]))
        self.underflow = 0

    def finish(self):
        self.output.write(bytes([0]))

class ArithmeticDecoder(ArithmeticCoderBase):
    def __init__(self, num_bits, inp):
        super().__init__(num_bits)
        self.input = inp
        self.code = 0
        self.current_byte = 0
        self.bits_read = 0

    def read(self, freq):
        total = freq.get_total()
        range_ = self.high - self.low + 1
        offset = self.code - self.low
        value = ((offset + 1) * total - 1) // range_
        # Binary search to find symbol
        low, high = 0, freq.get_symbol_limit()
        while low + 1 < high:
            mid = (low + high) // 2
            if freq.get_low(mid) > value:
                high = mid
            else:
                low = mid
        symbol = low
        low_count = freq.get_low(symbol)
        high_count = freq.get_high(symbol)
        self.high = self.low + (range_ * high_count // total) - 1
        self.low = self.low + (range_ * low_count // total)
        while True:
            if self.high < self.half_range:
                pass
            elif self.low >= self.half_range:
                self.low -= self.half_range
                self.high -= self.half_range
                self.code -= self.half_range
            elif self.low >= self.quarter_range and self.high < self.quarter_range * 3:
                self.low -= self.quarter_range
                self.high -= self.quarter_range
                self.code -= self.quarter_range
            else:
                break
            self.low = self.low << 1 & self.state_mask
            self.high = (self.high << 1 & self.state_mask) | 1
        return symbol

class FrequencyTable:
    def get_symbol_limit(self):
        raise NotImplementedError()
    def get(self, symbol):
        raise NotImplementedError()
    def get_total(self):
        raise NotImplementedError()
    def get_low(self, symbol):
        raise NotImplementedError()
    def get_high(self, symbol):
        raise NotImplementedError()
    def increment(self, symbol):
        raise NotImplementedError()

class SimpleFrequencyTable(FrequencyTable):
    def __init__(self, freqs):
        self.freqs = list(freqs)
        self.cum_freqs = None
        self._build_cumulative()

    def _build_cumulative(self):
        self.cum_freqs = [0]
        total = 0
        for f in self.freqs:
            total += f
            self.cum_freqs.append(total)

    def get_symbol_limit(self):
        return len(self.freqs)

    def get(self, symbol):
        return self.freqs[symbol]

    def get_total(self):
        return self.cum_freqs[-1]

    def get_low(self, symbol):
        return self.cum_freqs[symbol]

    def get_high(self, symbol):
        return self.cum_freqs[symbol+1]

    def increment(self, symbol):
        self.freqs[symbol] += 1
        self._build_cumulative()
