module ActiveDataFrame

  class Bounds < Struct.new(:from, :to, :length, :index); end
  class Point  < Struct.new(:index, :offset, :position);  end
  class DataFrameProxy
    attr_accessor :block_type, :data_frame_type, :block_type_name, :value_map
    def initialize(block_type, data_frame_type, value_map: nil)
      self.block_type      = block_type
      self.data_frame_type = data_frame_type
      self.block_type_name = block_type.table_name.gsub(/_blocks$/,'').gsub(/^blocks_/,'')
      self.value_map       = value_map
    end

    def reverse_value_map
      @reverse_value_map ||= value_map.invert
    end

    def [](*ranges)
      result = get(extract_ranges(ranges))
      if @value_map
        result.to_a.map{|row| row.kind_of?(Array) ? row.map(&reverse_value_map.method(:[])) : reverse_value_map[row]}
      else
        result
      end
    end

    def []=(from, values)
      values = Array(values).flatten.map(&@value_map.method(:[])) if @value_map
      from = column_map[from] if column_map && column_map[from]
      set(from, M[values, typecode: block_type::TYPECODE].to_a.flatten)
    end

    def column_map
      data_frame_type.column_map(data_frame_type.singular_df_name)
    end

    def column_name_map
      data_frame_type.column_name_map(data_frame_type.singular_df_name)
    end

    def reverse_column_map
      data_frame_type.reverse_column_map(data_frame_type.singular_df_name)
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
        else raise "Unexpected index for data frame proxy #{range}, expecting either a Range or an Integer"
        end
      end
    end

    def range_size
      0
    end

    def flatten_ranges(ranges)
    end

    def unmap_ranges(ranges, map)
      ranges.map do |range|
        case range
        when Range
          first       = (map[range.first] rescue nil) || range.first
          ends        = (map[range.end] rescue nil) || range.end
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
      return Bounds.new(
        Point.new(from_block_index, from_block_offset, from),
        Point.new(to_block_index,   to_block_offset, to),
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

    def match_range(from, to)
      from == to ? from : from..to
    end

    def blocks_between(bounds, block_scope: scope)
      bounds[1..-1].reduce(
        block_scope.where( block_type.table_name => { period_index: match_range(bounds[0].from.index,bounds[0].to.index)})
      ) do | or_chain, bound|
        or_chain.or(block_scope.where( block_type.table_name => { period_index: match_range(bound.from.index,bound.to.index)}))
      end
    end
  end
end