module ActiveDataFrame
  class Table < DataFrameProxy

    def initialize(block_type, data_frame_type)
      super
      self.send(:ids)
    end

    def set(from, values)
      data_frame_type.find_each do |instance|
        Row.new(self.block_type, self.data_frame_type, instance).set(from, values)
      end
    end

    def inspect
      "#{data_frame_type.name} Table"
    end

    def get(ranges)
      all_bounds = ranges.map.with_index do |range, index|
        get_bounds(range.first, range.exclude_end? ? range.end - 1 : range.end, index)
      end
      index_map = ids.each_with_index.to_h

      self.class.suppress_logs do
        existing_blocks = Hash.new{|h, index| h[index] = {}}
        blocks_between(all_bounds).pluck(:period_index, :data_frame_id, *block_type::COLUMNS).each do |pi, data_frame_id, *values|
          existing_blocks[pi][data_frame_id] = values
        end

        result = M.blank(columns: all_bounds.map(&:length).sum, rows: ids.count)

        iterate_bounds(all_bounds) do |index, left, right, cursor, size|
          if blocks = existing_blocks[index]
            blocks.each do |data_frame_id, block|
              row = index_map[data_frame_id]
              chunk = block[left..right]
              result.narray[cursor...cursor + size, row] = chunk
            end
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
        result.row_map = Hash.new do |h ,k|
          h[k] = begin
            case k
            when ActiveRecord::Base then index_map[k.id]
            when ActiveRecord::Relation then k.pluck(:id).map{|i| index_map[i] }
            when Numeric then index_map[k]
            end
          end
        end
        result
      end
    end

    def sum_gte(*ranges, max)
      select_agg_indices(extract_ranges(ranges), 'SUM', ->(x, y){ x <= y } , 'SUM(%) > :max', max: max)
    end

    def sum_lte(*ranges, min)
      select_agg_indices(extract_ranges(ranges), 'SUM', ->(x, y){ x >= y } , 'SUM(%) < :min', min: min)
    end

    def gte(*ranges, max)
      #TODO
    end

    def lte(*ranges, max)
      #TODO
    end

    def avg(*ranges)
      aggregate(extract_ranges(ranges), 'AVG')
    end

    def sum(*ranges)
      aggregate(extract_ranges(ranges), 'SUM')
    end

    def max(*ranges)
      aggregate(extract_ranges(ranges), 'MAX')
    end

    def min(*ranges)
      aggregate(extract_ranges(ranges), 'MIN')
    end

    def anon_table(period_index, column_query)
      @@sequence ||= 0
      next_seq = (@@sequence += 1) % 8192
      table_name = "db#{next_seq}"
      [
        "LEFT JOIN #{block_type.table_name} as #{table_name} ON #{table_name}.data_frame_id = #{data_frame_type.table_name}.id AND #{table_name}.data_frame_type = '#{data_frame_type.name}'",
        { table_name => { period_index: period_index }},
        { table_name => column_query}
      ]
    end

    def ar_sum(key)
      idx = column_map ? column_map[key] || key : key
      block_index  = idx / block_type::BLOCK_SIZE
      block_offset = (idx % block_type::BLOCK_SIZE).succ
      binding.pry
    end

    def ar_average(key)
      idx = column_map ? column_map[key] || key : key
      block_index  = idx / block_type::BLOCK_SIZE
      block_offset = (idx % block_type::BLOCK_SIZE).succ
    end

    def ar_minimum(key)
      idx = column_map ? column_map[key] || key : key
      block_index  = idx / block_type::BLOCK_SIZE
      block_offset = (idx % block_type::BLOCK_SIZE).succ
    end

    def ar_maximum(key)
      idx = column_map ? column_map[key] || key : key
      block_index  = idx / block_type::BLOCK_SIZE
      block_offset = (idx % block_type::BLOCK_SIZE).succ
    end


    def map_conditions_to_joins_and_query(conditions)
      conditions.map do |key, value|
        idx = column_map ? column_map[key] || key : key
        block_index  = idx / block_type::BLOCK_SIZE
        block_offset = (idx % block_type::BLOCK_SIZE).succ
        anon_table(block_index, "t#{block_offset}" => value)
      end
    end

    def where(conditions=nil, *params)
      condition_mapper = method(:map_conditions_to_joins_and_query)
      data_frame_type      = self.data_frame_type
      case conditions
      when nil
        Object.new.tap do |np|
          np.define_singleton_method(:not) do |not_conditions|
            condition_mapper[not_conditions]
              .reduce(data_frame_type){|agg, (join, block, cond)|
                agg.joins(join).where(block).where.not(cond)
              }
          end
        end
      else
        condition_mapper[conditions].reduce(data_frame_type){|agg, (join, block, cond)|
          agg.joins(join).where(block).where(cond) }
      end
    end

    def scope
      @scope ||= block_type.where(data_frame_type: data_frame_type.name).where(data_frame_id: ids)
    end

    private

      def ids
        @ids ||= data_frame_type.pluck(:id).sort
      end

      def select_agg_indices(ranges, agg, filter, condition, **args)
        all_bounds = ranges.map.with_index do |range, index|
          get_bounds(range.first, range.exclude_end? ? range.end - 1 : range.end, index)
        end
        existing = self.class.suppress_logs do
          blocks_between(all_bounds)
            .group(:period_index)
            .having(
              block_type::COLUMNS.map do |cl|
                condition.gsub('%', cl)
              end.join(" OR "),
              **args
            )
            .pluck(
              :period_index,
              *block_type::COLUMNS.map do |cl|
                "#{agg}(#{cl}) as #{cl}"
              end
            )
            .map{|pi, *values| [pi, values]}.to_h
        end
        indices = existing.flat_map do |period_index, *values|
          index = block_type::BLOCK_SIZE * period_index - 1
          M[values].mask{|x|
            index += 1
            !all_bounds.any?{|b| (b.from.position..b.to.position).include?(index) } || filter[x, args.values.first ]
          }.where.to_a.map{|v| block_type::BLOCK_SIZE * period_index + v}.to_a
        end

        if column_map
          indices.map{|i| reverse_column_map[i.to_i] }
        else
          indices
        end
      end

      def aggregate(ranges, agg)
        all_bounds = ranges.map.with_index do |range, index|
          get_bounds(range.first, range.exclude_end? ? range.end - 1 : range.end, index)
        end

        self.class.suppress_logs do
          existing = blocks_between(all_bounds)
                      .group(:period_index)
                      .pluck(:period_index, *block_type::COLUMNS.map{|cl| "#{agg}(#{cl}) as #{cl}"})
                      .map{|pi, *values| [pi, values]}.to_h
          result = M.blank(columns: all_bounds.map(&:length).sum)

          iterate_bounds(all_bounds) do |index, left, right, cursor, size|
            if block = existing[index]
              chunk = block[left..right]
              result.narray[cursor...cursor + size] = chunk.length == 1 ? chunk.first : chunk
            end
          end
          result.column_map = column_map if column_map
          result
        end
      end
  end
end