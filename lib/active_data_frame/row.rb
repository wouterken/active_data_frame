module ActiveDataFrame
  class Row < DataFrameProxy

    attr_accessor :instance

    def initialize(block_type, data_frame_type, instance)
      super(block_type, data_frame_type)
      self.instance = instance
    end

    def inspect
      "#{data_frame_type.name} Row(#{instance.id})"
    end

    def set(from, values)
      to     = (from + values.length) - 1
      bounds = get_bounds(from, to)

      self.class.suppress_logs do
        new_blocks = Hash.new do |h, k|
          h[k] = [[0] * block_type::BLOCK_SIZE]
        end

        existing = blocks_between([bounds]).pluck(:id, :period_index, *block_type::COLUMNS).map do |id, period_index, *block_values|
          [period_index, [block_values, id]]
        end.to_h

        iterate_bounds([bounds]) do |index, left, right, cursor, size|
          chunk = values[cursor...cursor + size]
          block = existing[index] || new_blocks[index]
          block.first[left..right] = chunk.to_a
        end

        bulk_update(existing) unless existing.size.zero?
        bulk_insert(new_blocks) unless new_blocks.size.zero?
        values
      end
    end

    def get(ranges)
      all_bounds = ranges.map.with_index do |range, index|
        get_bounds(range.first, range.exclude_end? ? range.end - 1 : range.end, index)
      end
      existing = blocks_between(all_bounds).pluck(:period_index, *block_type::COLUMNS).map{|pi, *values| [pi, values]}.to_h
      result   = M.blank(columns: all_bounds.map(&:length).sum)

      iterate_bounds(all_bounds) do |index, left, right, cursor, size|
        if block = existing[index]
          chunk = block[left..right]
          result.narray[cursor...cursor + size] = chunk.length == 1 ? chunk.first : chunk
        end
      end

      if column_map
        total = 0
        range_sizes = ranges.map do |range, memo|
          last_total = total
          total += range.size
          [range.first, range.size, last_total]
        end
        index_of = ->(column){
          selected = range_sizes.find{|start, size, total| start <= column && start + size >= column}
          if selected
            start, size, total = selected
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
      ##
      # Update block data for all blocks in a single call
      ##
      def bulk_update(existing)
        case ActiveRecord::Base.connection_config[:adapter]
        when 'postgresql'
          # Fast bulk update
          updates = ''
          existing.each do |period_index, (values, id)|
            updates <<  "(#{id}, #{values.join(',')}),"
          end
          perform_update(updates)
        else
          ids = existing.map {|_, (_, id)| id}
          updates = block_type::COLUMNS.map.with_index do |column, column_idx|
            [column, "CASE period_index\n#{existing.map{|period_index, (values, id)| "WHEN #{period_index} then #{values[column_idx]}"}.join("\n")} \nEND\n"]
          end.to_h
          update_statement = updates.map{|cl, up| "#{cl} = #{up}" }.join(', ')
          block_type.connection.execute("UPDATE #{block_type.table_name} SET #{update_statement} WHERE #{block_type.table_name}.id IN (#{ids.join(',')});")
        end
      end

      ##
      # Insert block data for all blocks in a single call
      ##
      def bulk_insert(new_blocks)
        inserts = ''
        new_blocks.each do |period_index, (values)|
          inserts << \
          case ActiveRecord::Base.connection_config[:adapter]
          when 'postgresql', 'mysql2' then "(#{values.join(',')}, #{instance.id}, #{period_index}, '#{data_frame_type.name}', now(), now()),"
          else "(#{values.join(',')}, #{instance.id}, #{period_index}, '#{data_frame_type.name}', datetime(), datetime()),"
          end
        end
        perform_insert(inserts)
      end

      def perform_update(updates)
        block_type.transaction do
          block_type.connection.execute(
            "UPDATE #{block_type.table_name} SET #{block_type::COLUMNS.map{|col| "#{col} = t.#{col}" }.join(", ")} FROM(VALUES #{updates[0..-2]}) as t(id, #{block_type::COLUMNS.join(',')}) WHERE #{block_type.table_name}.id = t.id"
          )
        end
        true
      end

      def perform_insert(inserts)
        sql = "INSERT INTO #{block_type.table_name} (#{block_type::COLUMNS.join(',')}, data_frame_id, period_index, data_frame_type, created_at, updated_at) VALUES #{inserts[0..-2]}"
        block_type.connection.execute sql
      end

      def scope
        @scope ||= block_type.where(data_frame_type: data_frame_type.name, data_frame_id: instance.id)
      end
  end
end