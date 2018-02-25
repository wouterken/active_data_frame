module ActiveDataFrame
  class DatabaseConfig

    def self.execute(sql)
      if defined?(@@batching) && @@batching
        @@batch << sql << ?;
      else
        ActiveRecord::Base.transaction do
          ActiveRecord::Base.connection.execute sql
        end
      end
    end

    def self.flush!
      execute(@@batch)
      @@batch = ''
    end

    def self.for_types(block:, df:)
      (@@configs ||= {})[[block, df]] ||= DatabaseConfig.new(block, df)
    end

    attr_reader :block_type, :data_frame_type

    def initialize(block_type, data_frame_type)
      @block_type = block_type
      @data_frame_type = data_frame_type
    end

    def self.batch
      @@batching, prev_batch = true, defined?(@@batching) ? @@batching : false
      @@batch ||= ''
      ActiveRecord::Base.transaction do
        yield
      end
      @@batching = prev_batch
      flush! unless @@batching
    end
    ##
    # Update block data for all blocks in a single call
    ##
    def bulk_update(existing)
      case ActiveRecord::Base.connection_config[:adapter]
      when 'postgresql'.freeze
        # Fast bulk update
        updates = ''
        existing.each do |period_index, (values, df_id)|
          updates <<  "(#{df_id}, #{period_index}, #{values.map{|v| v.inspect.gsub('"',"'") }.join(',')}),"
        end
        perform_update(updates)
      else
        ids = existing.map {|_, (_, id)| id}
        updates = block_type::COLUMNS.map.with_index do |column, column_idx|
          [column, "CASE period_index\n#{existing.map{|period_index, (values, _)| "WHEN #{period_index} then #{values[column_idx]}"}.join("\n")} \nEND\n"]
        end.to_h
        update_statement = updates.map{|cl, up| "#{cl} = #{up}" }.join(', ')
        DatabaseConfig.execute("UPDATE #{block_type.table_name} SET #{update_statement} WHERE
          #{block_type.table_name}.data_frame_id IN (#{ids.join(',')})
          AND #{block_type.table_name}.data_frame_type = '#{data_frame_type.name}'
          AND #{block_type.table_name}.period_index IN (#{existing.keys.join(', ')});
          "
        )
      end
    end

    ##
    # Insert block data for all blocks in a single call
    ##
    def bulk_insert(new_blocks, instance)
      inserts = ''
      new_blocks.each do |period_index, (values)|
        inserts << \
        case ActiveRecord::Base.connection_config[:adapter]
        when 'postgresql', 'mysql2' then "(#{values.map{|v| v.inspect.gsub('"',"'") }.join(',')}, #{instance.id}, #{period_index}, '#{data_frame_type.name}', now(), now()),"
        else "(#{values.map{|v| v.inspect.gsub('"',"'") }.join(',')}, #{instance.id}, #{period_index}, '#{data_frame_type.name}', datetime(), datetime()),"
        end
      end
      perform_insert(inserts)
    end

    def perform_update(updates)
      DatabaseConfig.execute(
        <<-SQL
        UPDATE #{block_type.table_name}
          SET #{block_type::COLUMNS.map{|col| "#{col} = t.#{col}" }.join(", ")}
          FROM(
          VALUES #{updates[0..-2]}) as t(data_frame_id, period_index, #{block_type::COLUMNS.join(',')})
          WHERE #{block_type.table_name}.data_frame_id = t.data_frame_id
          AND #{block_type.table_name}.period_index = t.period_index
          AND #{block_type.table_name}.data_frame_type = '#{data_frame_type.name}'
        SQL
      )
      true
    end

    def perform_insert(inserts)
      sql = "INSERT INTO #{block_type.table_name} (#{block_type::COLUMNS.join(',')}, data_frame_id, period_index, data_frame_type, created_at, updated_at) VALUES #{inserts[0..-2]}"
      DatabaseConfig.execute sql
    end
  end
end