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

    def self.set_all(scope, block_type, data_frame_type, from, values, trim: false)
      if trim || ActiveRecord::Base.connection_config[:adapter] === 'mysql2'
        scope.each do |instance|
          Row.new(block_type, data_frame_type, instance).patch(from, values.kind_of?(Hash) ? values[instance.id] : values)
        end
      else
        upsert_all(scope, block_type, data_frame_type, from, values)
      end
    end

    def self.upsert_all(rows, block_type, data_frame_type, from, values)
      length                 = values.kind_of?(Hash) ? values.values.first.length : values.length
      to                     = from + length - 1
      bounds                 = get_bounds(from, to, block_type)
      scope                  = block_type.where(data_frame_type: data_frame_type.name, data_frame_id: rows.select(:id))
      scope                  = scope.where(data_frame_id: values.keys) if values.kind_of?(Hash)
      all_update_indices     = scope.where(period_index: bounds.from.index..bounds.to.index).order(data_frame_id: :asc, period_index: :asc).pluck(:data_frame_id, :period_index)
      grouped_update_indices = all_update_indices.group_by(&:first).transform_values{|value| Set.new(value.map!(&:last)) }
      instance_ids           = rows.pluck(:id)
      instance_ids           &= values.keys if values.kind_of?(Hash)
      upserts = to_enum(:iterate_bounds, [bounds], block_type).flat_map do |index, left, right, cursor, size|
        instance_ids.map do |instance_id|
          slice = values.kind_of?(Hash) ? values[instance_id][cursor...cursor + size] : values[cursor...cursor + size]
          [[:data_frame_id, instance_id], [:period_index, index], *(left.succ..right.succ).map{|v| :"t#{v}" }.zip(slice)].to_h
        end
      end

      update, insert = upserts.partition{|upsert| grouped_update_indices[upsert[:data_frame_id]]&.include?(upsert[:period_index]) }
      Database.for_types(block: block_type, df: data_frame_type).bulk_upsert(update, insert)
      values
    end

    def set(from, values, trim: false)
      if trim || ActiveRecord::Base.connection_config[:adapter] === 'mysql2'
        patch(from, values)
      else
        upsert(from, values)
      end
    end

    def upsert(from, values)
      to             = (from + values.length) - 1
      bounds         = get_bounds(from, to)
      update_indices = Set.new(scope.where(period_index: bounds.from.index..bounds.to.index).order(period_index: :asc).pluck(:period_index))
      # Detect blocks in bounds:
      # - If existing and covered, do an update without load
      # - If existing and uncovered, do a small write (without load)
      # - If not existing, insert!
      upserts = to_enum(:iterate_bounds, [bounds]).map do |index, left, right, cursor, size|
        [[:data_frame_id, self.instance.id], [:period_index, index], *(left.succ..right.succ).map{|v| :"t#{v}" }.zip(values[cursor...cursor + size])].to_h
      end
      update, insert = upserts.partition{|upsert| update_indices.include?(upsert[:period_index]) }
      database.bulk_upsert(update, insert)
      values
    end

    def patch(from, values)
      to     = (from + values.length) - 1
      bounds = get_bounds(from, to)

      new_blocks = Hash.new do |h, k|
        h[k] = [[0] * block_type::BLOCK_SIZE, self.instance.id]
      end

      deleted_indices = []

      existing = blocks_between([bounds]).pluck(:data_frame_id, :period_index, *block_type::COLUMNS).map do |id, period_index, *block_values|
        [period_index, [block_values, id]]
      end.to_h

      iterate_bounds([bounds]) do |index, left, right, cursor, size|
        chunk = values[cursor...cursor + size]
        if existing[index]
          block = existing[index]
          block.first[left..right] = chunk.to_a
          if block.first.all?(&:zero?)
            deleted_indices << index
            existing.delete(index)
          end
        elsif chunk.any?(&:nonzero?)
          new_blocks[index].first[left..right] = chunk.to_a
        end
      end


      database.bulk_delete(self.instance.id, deleted_indices) unless deleted_indices.size.zero?
      database.bulk_update(existing)       unless existing.size.zero?
      database.bulk_insert(new_blocks)     unless new_blocks.size.zero?
      values
    end

    def get(ranges)
      all_bounds = ranges.map.with_index do |range, index|
        get_bounds(range.first, range.exclude_end? ? range.end - 1 : range.end, index)
      end

      existing = self.class.suppress_logs{
        blocks_between(all_bounds).pluck(:period_index, *block_type::COLUMNS).map{|pi, *values| [pi, values]}.to_h
      }
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