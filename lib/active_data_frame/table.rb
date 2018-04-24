module ActiveDataFrame
  class Table < DataFrameProxy

    def set(from, values)
      ActiveDataFrame::Database.batch do
        data_frame_type.each do |instance|
          Row.new(self.block_type, self.data_frame_type, instance).set(from, values)
        end
      end
    end

    def inspect
      "#{data_frame_type.name} Table"
    end

    def build_case_map(all_bounds)
      map = block_type::COLUMNS.map{|col| [col, []]}.to_h

      all_bounds.each do |bound|
        case bound.from.index
        when bound.to.index
          (bound.from.offset+1..bound.to.offset+1).each do |col_idx|
            map["t#{col_idx}"] << (bound.from.index..bound.from.index)
          end
        else
          (bound.from.offset+1..block_type::COLUMNS.size).each do |col_idx|
            map["t#{col_idx}"] << (bound.from.index..bound.from.index)
          end
          (1..block_type::COLUMNS.size).each do |col_idx|
            map["t#{col_idx}"] << (bound.from.index.succ..bound.to.index-1)
          end if bound.from.index.succ != bound.to.index
          (1..bound.to.offset+1).each do |col_idx|
            map["t#{col_idx}"] << (bound.to.index..bound.to.index)
          end
        end
      end
      map
    end

    def column_cases(cases, aggregation_function=nil)
      block_type::COLUMNS.map do |col|
        col_cases = cases[col].sort_by(&:begin).reduce([]) do |agg, col_case|
          if agg.empty?
            agg << col_case
            agg
          else
            if agg[-1].end.succ == col_case.begin
              agg[-1] = (agg[-1].begin..col_case.end)
            else
              agg << col_case
            end
            agg
          end
        end

        if aggregation_function
          case col_cases.length
          when 0 then "NULL::float as #{col}"
          else
            case_str = col_cases.map do |match|
              case
              when match.begin == match.end then "period_index = #{match.begin}"
              else "period_index BETWEEN #{match.begin} AND #{match.end}"
              end
            end.join(" OR ")
            "CASE WHEN #{case_str} THEN #{aggregation_function}(#{col}) ELSE NULL END"
          end
        else
          case col_cases.length
          when 0 then "NULL as #{col}"
          else
            case_str = col_cases.map do |match|
              case
              when match.begin == match.end then "period_index = #{match.begin}"
              else "period_index BETWEEN #{match.begin} AND #{match.end}"
              end
            end.join(" OR ")
            "CASE WHEN #{case_str} THEN #{col} ELSE NULL END"
          end
        end
      end.map(&Arel.method(:sql))
    end

    def get(ranges)
      ranges = extract_ranges(ranges)
      all_bounds = ranges.map.with_index do |range, index|
        get_bounds(range.first, range.exclude_end? ? range.end - 1 : range.end, index)
      end

      case_map  = build_case_map(all_bounds)

      existing_blocks = Hash.new{|h, index| h[index] = {}}

      index_map = {}
      res = ActiveRecord::Base.transaction do
        ids = data_frame_type.pluck(:id)
        as_sql = blocks_between(
          all_bounds,
          block_scope: data_frame_type.unscoped
                                    .joins("LEFT JOIN #{block_type.table_name} ON #{data_frame_type.table_name}.id = #{block_type.table_name}.data_frame_id")
                                    .joins("RIGHT JOIN (#{data_frame_type.select(:id).to_sql}) as ref ON ref.id = #{block_type.table_name}.data_frame_id")

        ).where(
          block_type.table_name => {data_frame_type: data_frame_type.name }
        ).select(:period_index, :data_frame_id, *column_cases(case_map)).to_sql

        index_map = ids.each_with_index.to_h
        ActiveRecord::Base.connection.execute(as_sql)
      end

      res.each_row do |pi, data_frame_id, *values|
        existing_blocks[pi][data_frame_id] = values
      end

      result = M.blank(typecode: block_type::TYPECODE, columns: all_bounds.map(&:length).sum, rows: index_map.size)
      iterate_bounds(all_bounds) do |index, left, right, cursor, size|
        if blocks = existing_blocks[index]
          blocks.each do |data_frame_id, block|
            row = index_map[data_frame_id]
            next unless row
            chunk = block[left..right]
            result.narray[cursor...cursor + size, row] = chunk
          end
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
      result.row_map = Hash.new do |h ,k|
        h[k] = begin
          case k
          when ActiveRecord::Base then index_map[k.id]
          when ActiveRecord::Relation then k.pluck(:id).map{|i| index_map[i] }
          when ->(list){ list.kind_of?(Array) && list.all?{|v| v.kind_of?(ActiveRecord::Base)}} then k.map{|i| index_map[i.id] }
          when Numeric then index_map[k]
          end
        end
      end
      result
    end

    def idx_where_sum_gte(*ranges, max)
      select_agg_indices(extract_ranges(ranges), 'SUM', ->(x, y){ x <= y } , 'SUM(%) >= :max', max: max)
    end

    def idx_where_sum_lte(*ranges, min)
      select_agg_indices(extract_ranges(ranges), 'SUM', ->(x, y){ x >= y } , 'SUM(%) <= :min', min: min)
    end

    def AggregateProxy(agg)
      proxy = Object.new
      aggregate, extract_ranges = method(:aggregate), method(:extract_ranges)
      proxy.define_singleton_method(:[]) do |*ranges|
        aggregate[extract_ranges[ranges], agg]
      end
      proxy
    end

    def avg
      @avg ||= AggregateProxy('AVG')
    end

    def sum
      @sum ||= AggregateProxy('SUM')
    end

    def max
      @max ||= AggregateProxy('MAX')
    end

    def min
      @min ||= AggregateProxy('MIN')
    end

    private

      def scope
        @scope ||= block_type.where(data_frame_type: data_frame_type.name, data_frame_id: data_frame_type.select(:id))
      end

      def select_agg_indices(ranges, agg, filter, condition, **args)
        all_bounds = ranges.map.with_index do |range, index|
          get_bounds(range.first, range.exclude_end? ? range.end - 1 : range.end, index)
        end
        existing = blocks_between(all_bounds)
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
              Arel.sql("#{agg}(#{cl}) as #{cl}")
            end
          )
          .map{|pi, *values| [pi, values]}.to_h
        indices = existing.flat_map do |period_index, *values|
          index = block_type::BLOCK_SIZE * period_index - 1
          M[values, typecode: block_type::TYPECODE].mask{|x|
            index += 1
            !all_bounds.any?{|b| (b.from.position..b.to.position).include?(index) } || filter[x, args.values.first ]
          }.where.to_a.map{|v| block_type::BLOCK_SIZE * period_index + v}.to_a
        end

        if column_map
          indices.map{|i| reverse_column_map[i.to_i] || i.to_i }
        else
          indices
        end
      end

      def aggregate(ranges, agg)
        all_bounds = ranges.map.with_index do |range, index|
          get_bounds(range.first, range.exclude_end? ? range.end - 1 : range.end, index)
        end

        case_map  = build_case_map(all_bounds)
        existing  = blocks_between(all_bounds).group(:period_index).pluck(:period_index, *column_cases(case_map, agg))
                    .map{|pi, *values| [pi, values]}.to_h
        result = M.blank(columns: all_bounds.map(&:length).sum, typecode: block_type::TYPECODE)

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