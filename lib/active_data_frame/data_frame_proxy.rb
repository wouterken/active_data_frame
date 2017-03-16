module ActiveDataFrame
  class DataFrameProxy
    attr_accessor :block_type, :data_frame_type, :block_type_name
    def initialize(block_type, data_frame_type)
      self.block_type  = block_type
      self.data_frame_type = data_frame_type
      self.block_type_name = block_type.table_name.gsub(/_blocks$/,'')
    end

    def [](*ranges)
      get(extract_ranges(ranges))
    end

    def []=(from, values)
      from = column_map ? column_map[from] : from
      set(from, M[values].to_a.flatten)
    end

    def column_map
      data_frame_type.column_map(self.block_type_name)
    end

    def column_name_map
      data_frame_type.column_name_map(self.block_type_name)
    end

    def reverse_column_map
      data_frame_type.reverse_column_map(self.block_type_name)
    end

    def method_missing(name, *args, &block)
      if column_name_map && column_map[name]
        self[name]
      else
        super
      end
    end

    def extract_ranges(ranges)
      ranges = unmap_ranges(ranges, column_map) if column_map
      ranges.map do |range|
        case range
        when Range then range
        when Fixnum then range..range
        else raise "Unexpected index #{range}"
        end
      end
    end

    def unmap_ranges(ranges, map)
      ranges.map do |range|
        case range
        when Range
          first       = map[range.first] || range.first
          ends        = map[range.end] || range.end
          range.exclude_end? ? first...ends : first..ends
        else map[range] || range
        end
      end
    end

    def get_bounds(from, to, index=0)
      from_block_index  = from / block_type::BLOCK_SIZE
      from_block_offset = from % block_type::BLOCK_SIZE
      to_block_index    = to / block_type::BLOCK_SIZE
      to_block_offset   = to % block_type::BLOCK_SIZE
      return Struct.new(:from, :to, :length, :index).new(
        Struct.new(:index, :offset, :position).new(from_block_index, from_block_offset, from),
        Struct.new(:index, :offset, :position).new(to_block_index,   to_block_offset, to),
        (to - from) + 1,
        index
      )
    end

    def self.suppress_logs
      ActiveRecord::Base.logger, old_logger = nil,  ActiveRecord::Base.logger
      yield.tap do
        ActiveRecord::Base.logger = old_logger
      end
    end

    def iterate_bounds(all_bounds)
      cursor = 0
      all_bounds.each do |bounds|
        index = bounds.from.index
        while index <= bounds.to.index
          left  = index == bounds.from.index ? bounds.from.offset : 0
          right = index == bounds.to.index   ? bounds.to.offset   : block_type::BLOCK_SIZE - 1
          size  = (right - left)+1
          yield index, left, right, cursor, size
          cursor += size
          index += 1
        end
      end
    end

    def blocks_between(bounds)
      bounds[1..-1].reduce(
        scope.where(period_index: (bounds[0].from.index..bounds[0].to.index).to_a)
      ) do | or_chain, bound|
        or_chain.or(scope.where(period_index: (bound.from.index..bound.to.index).to_a))
      end
    end
  end
end