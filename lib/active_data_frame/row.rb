module ActiveDataFrame
  class Row < DataFrameProxy

    attr_accessor :instance

    def initialize(block_type, data_frame_type, instance, value_map: nil, singular_df_name: '', plural_df_name: '')
      super(block_type, data_frame_type, value_map: value_map, singular_df_name: singular_df_name, plural_df_name: plural_df_name)
      self.instance = instance
    end

    def inspect
      "#{data_frame_type.name} Row(#{instance.id})"
    end

    def set(from, values)
      to     = (from + values.length) - 1
      bounds = get_bounds(from, to)

      new_blocks = Hash.new do |h, k|
        h[k] = [[0] * block_type::BLOCK_SIZE]
      end

      deleted_indices = []

      existing = blocks_between([bounds]).pluck(:data_frame_id, :period_index, *block_type::COLUMNS).map do |id, period_index, *block_values|
        [period_index, [block_values, id]]
      end.to_h

      iterate_bounds([bounds]) do |index, left, right, cursor, size|
        chunk = values[cursor...cursor + size]
        if size == block_type::BLOCK_SIZE && chunk.all?(&:zero?)
          deleted_indices << index
        else
          block = existing[index] || new_blocks[index]
          block.first[left..right] = chunk.to_a
        end
      end

      database.bulk_delete(self.id, deleted_indices) unless deleted_indices.size.zero?
      database.bulk_update(existing)                 unless existing.size.zero?
      database.bulk_insert(new_blocks, instance)     unless new_blocks.size.zero?
      values
    end

    def get(ranges)
      all_bounds = ranges.map.with_index do |range, index|
        get_bounds(range.first, range.exclude_end? ? range.end - 1 : range.end, index)
      end

      existing = blocks_between(all_bounds).pluck(:period_index, *block_type::COLUMNS).map{|pi, *values| [pi, values]}.to_h
      result   = M.blank(typecode: block_type::TYPECODE, columns: all_bounds.map(&:length).sum)

      iterate_bounds(all_bounds) do |index, left, right, cursor, size|
        if block = existing[index]
          chunk = block[left..right]
          result.narray[cursor...cursor + size] = chunk.length == 1 ? chunk.first : chunk
        end
      end

      if column_map && !column_map.default_proc
        total = 0
        range_sizes = ranges.map do |range, memo|
          last_total = total
          total += range.size
          [range.first, range.size, last_total]
        end
        index_of = ->(column){
          selected = range_sizes.find{|start, size| start <= column && start + size >= column}
          if selected
            start, _, total = selected
            (column - start) + total
          else
            nil
          end
        }
        result.column_map = column_map.map do |name, column|
          [name, index_of[column_map[name]]]
        end.to_h
      end
      result
    end

    private
      def scope
        @scope ||= block_type.where(data_frame_type: data_frame_type.name, data_frame_id: instance.id)
      end
  end
end