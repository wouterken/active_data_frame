module ActiveDataFrame
  class Database

    def self.batching
      !!Thread.current[:active_data_frame_batching]
    end

    def self.batching=(value)
      Thread.current[:active_data_frame_batching] = !!value
    end

    def self.execute(sql)
      if ActiveDataFrame::Database.batching
        Thread.current[:batch] << sql << ?;
      else
        unless sql.empty?
          ActiveRecord::Base.transaction do
            ActiveDataFrame::DataFrameProxy.suppress_logs do
              case ActiveRecord::Base.connection_config[:adapter]
              when 'sqlite3'.freeze
                ActiveRecord::Base.connection.raw_connection.execute_batch sql
              when 'mysql2'
                sql.split(';').reject{|x| x.strip.empty?}.each do |stmt|
                  ActiveRecord::Base.connection.execute(stmt)
                end
              else
                ActiveRecord::Base.connection.execute(sql)
              end
            end
          end
        end
      end
    end

    def self.flush!
      execute(Thread.current[:batch])
      Thread.current[:batch] = ''
    end

    def self.for_types(block:, df:)
      (@@configs ||= {})[[block, df]] ||= Database.new(block, df)
    end

    attr_reader :block_type, :data_frame_type

    def initialize(block_type, data_frame_type)
      @block_type = block_type
      @data_frame_type = data_frame_type
    end

    def self.batch
      self.batching, prev_batch = true, self.batching
      Thread.current[:batch] ||= ''
      ActiveRecord::Base.transaction do
        yield
      end
    ensure
      self.batching = prev_batch
      flush! unless self.batching
    end

    def bulk_upsert(updates, inserts)
      Database.batch do
        updates.group_by(&:keys).transform_values{|v| v.map(&:values) }.each do |columns, rows|
          update = rows.map{|df_id, period_index, *values| [period_index, [values, df_id]] }
          bulk_update(update, columns - [:data_frame_id, :period_index])
        end
        inserts.group_by(&:keys).transform_values{|v| v.map(&:values) }.each do |columns, rows|
          insert = rows.map{|df_id, period_index, *values| [period_index, [values, df_id]] }
          bulk_insert(insert, columns - [:data_frame_id, :period_index])
        end
      end
    end
    ##
    # Update block data for all blocks in a single call
    ##
    def bulk_update(existing, columns=block_type::COLUMNS)
      existing.each_slice(ActiveDataFrame.update_max_batch_size) do |existing_slice|
        # puts "Updating slice of #{existing_slice.length}"
        case ActiveRecord::Base.connection_config[:adapter]
        when 'postgresql'.freeze
          #
          # PostgreSQL Supports the fast setting of multiple update values that differ
          # per row from a temporary table.
          #
          updates = ''
          existing_slice.each do |period_index, (values, df_id)|
            updates <<  "(#{df_id}, #{period_index}, #{values.map{|v| v.inspect.gsub('"',"'") }.join(',')}),"
          end
          Database.execute(
            <<-SQL
            UPDATE #{block_type.table_name}
              SET #{columns.map{|col| "#{col} = t.#{col}" }.join(", ")}
              FROM(
              VALUES #{updates[0..-2]}) as t(data_frame_id, period_index, #{columns.join(',')})
              WHERE #{block_type.table_name}.data_frame_id = t.data_frame_id
              AND #{block_type.table_name}.period_index = t.period_index
              AND #{block_type.table_name}.data_frame_type = '#{data_frame_type.name}'
            SQL
          )
        #
        # For MySQL we use the ON DUPLICATE KEY UPDATE functionality.
        # This relies on there being a unique index dataframe and period index
        # on the blocks table.
        # This tends to be faster than the general CASE based solution below
        # but slower than the PostgreSQL solution above
        #
        when 'mysql2'.freeze
          # Fast bulk update
          updates, on_duplicate = "", ""
          existing_slice.each do |period_index, (values, df_id)|
            updates << "(#{values.map{|v| v.inspect.gsub('"',"'") }.join(',')}, #{df_id}, #{period_index}, '#{data_frame_type.name}'),"
          end
          on_duplicate = columns.map do |cname|
            "#{cname}=VALUES(#{cname})"
          end.join(", ")
          stmt = <<-SQL
            INSERT INTO #{block_type.table_name} (#{columns.join(',')},data_frame_id,period_index,data_frame_type)
            VALUES #{updates[0..-2]}
            ON DUPLICATE KEY UPDATE #{on_duplicate}
          SQL
          Database.execute(stmt)
        else
          #
          # General CASE based solution for multiple differing updates
          # set per row.
          # We use a CASE statement per column which determines the column
          # to set based on the period index
          #
          ids = existing_slice.map {|_, (_, id)| id}
          updates = columns.map.with_index do |column, column_idx|
            [column, "CASE \n#{existing_slice.map{|period_index, (values, df_id)| "WHEN period_index=#{period_index} AND data_frame_id=#{df_id} then #{values[column_idx]}" }.join("\n")} \nEND\n"]
          end.to_h
          update_statement = updates.map{|cl, up| "#{cl} = #{up}" }.join(', ')
          Database.execute(<<-SQL
            UPDATE #{block_type.table_name} SET #{update_statement} WHERE
            #{block_type.table_name}.data_frame_id IN (#{ids.join(',')})
            AND #{block_type.table_name}.data_frame_type = '#{data_frame_type.name}'
            AND #{block_type.table_name}.period_index IN (#{existing_slice.map(&:first).join(', ')});
          SQL
          )
        end
      end
    end

    def bulk_delete(id, indices)
      indices.each_slice(ActiveDataFrame.delete_max_batch_size) do |slice|
        # puts "Deleting slice of #{slice.length}"
        block_type.where(data_frame_id: id, period_index: slice).delete_all
      end
    end

    ##
    # Insert block data for all blocks in a single call
    ##
    def bulk_insert(new_blocks, columns=block_type::COLUMNS)
      new_blocks.each_slice(ActiveDataFrame.insert_max_batch_size) do |new_blocks_slice|
        # puts "Inserting slice of #{new_blocks_slice.length}"
        inserts = ''
        new_blocks_slice.each do |period_index, (values, df_id)|
          inserts << \
          case ActiveRecord::Base.connection_config[:adapter]
          when 'postgresql', 'mysql2' then "(#{values.map{|v| v.inspect.gsub('"',"'") }.join(',')}, #{df_id}, #{period_index}, '#{data_frame_type.name}'),"
          else "(#{values.map{|v| v.inspect.gsub('"',"'") }.join(',')}, #{df_id}, #{period_index}, '#{data_frame_type.name}'),"
          end
        end
        sql = "INSERT INTO #{block_type.table_name} (#{columns.join(',')}, data_frame_id, period_index, data_frame_type) VALUES #{inserts[0..-2]}"
        Database.execute sql
      end
    end
  end
end