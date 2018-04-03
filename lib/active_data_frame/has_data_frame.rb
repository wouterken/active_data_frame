module ActiveDataFrame
  #
  # Modules can include HasDataFrame('frame_name', FrameBlockType) to gain data frame capabilities
  # This method will expose class level and row (Active Record instance) level accessors to the underlying data frame.
  #
   # E.g.
  #
  # module HasBar
  #   include HasDataFrame('bars', BarBlock)
  # end
  #
  # class Foo
  #   include HasBar
  # end
  #
  # # Select all bars from index 0 to 40, for all foos
  # Foo.bars[0..40]
  #
   # Select all bars from index 0 to 40, for foo with id: 1
  # Foo.find(1).bars[0..40]
  #
  # # Find the average bar size for Foo 1 from index 5 to 30
  # Foo.find(1).bars[5..30].avg
  #
  # Find the average bar size for the first 10 foos from index 13..43
  # Foo.limit(10).bars.avg[13..43]
  #
  # Find the sum size for all foos wher baz == boo from index 13..43
  # Foo.where(baz: :boo).bars.sum[13..43]
  #
  def self.HasDataFrame(singular_table_name,  block_type, table_name: singular_table_name, value_map: nil, &block)
    Module.new do
      define_singleton_method(:included) do |base|
        # If somebody includes our dataframe enabled module we execute the following
        base.define_singleton_method(:included) do |decorated|
          block[decorated] if block
          decorated.extend(base::ClassMethods) if defined?(base::ClassMethods)

          # add our class level methods
          decorated.extend(
            ActiveDataFrame.build_module_class_methods(singular_table_name, block_type, table_name: table_name, value_map: value_map)
          )

          # Add our instance level methods
          decorated.class_eval do

            if value_map
              decorated.const_set(singular_table_name.underscore.camelize, ActiveDataFrame.build_dot_accessible_hash(value_map))
            end

            # Provide memoised reference to DF row
            define_method singular_table_name do
              (@data_frame_proxies ||= {})[singular_table_name] ||= Row.new(
                block_type,
                self.class,
                self,
                value_map: value_map,
                singular_df_name: singular_table_name,
                plural_df_name: table_name
              )
            end

            # We provide our own inspect implementation which will include in the output
            # selected dataframe attributes that do not reside on the parent table
            define_method :inspect do
              inspection = "not initialized"
              if defined?(@attributes) && @attributes
                 inspection = @attributes.keys.collect { |name|
                   if has_attribute?(name)
                     "#{name}: #{attribute_for_inspect(name)}"
                   end
                 }.compact.join(", ")
              end
              "<#{self.class} #{inspection}>"
            end
          end
        end
      end
    end
  end

  #
  # Define methods on our hash to easily access any values that are indexed by a symbol key
  # and that do not clash with existing methods on the Hash
  #
  def self.build_dot_accessible_hash(hash)
    hash.dup.tap do |map|
      map.each do |key, value|
        if(key.kind_of?(Symbol) && !hash.respond_to?(key))
          map.define_singleton_method(key){value}
        end
      end
    end
  end

  #
  # The class methods that are defined on any class the includes our dataframe enabled module
  #
  def self.build_module_class_methods(singular_table_name,  block_type, table_name: singular_table_name, value_map: nil)
    Module.new do

      # The key ADF functionality is exposed here.
      # This defines a new `table_name` accesor on the class which gives you access to a dataframe proxy by the name of `table_name`
      #
      # E.g.
      #
      # class Foo
      #   include HasBar
      # end
      #
      # # Select all bars from index 0 to 40, for all foos
      # Foo.bars[0..40]
      #
       # Select all bars from index 0 to 40, for foo with id: 1
      # Foo.find(1).bars[0..40]
      #
      # # Find the average bar size for Foo 1 from index 5 to 30
      # Foo.find(1).bars[5..30].avg
      #
      # Find the average bar size for the first 10 foos from index 13..43
      # Foo.limit(10).bars.avg[13..43]
      #
      # Find the sum size for all foos wher baz == boo from index 13..43
      # Foo.where(baz: :boo).bars.sum[13..43]
      #
      define_method(table_name) do
        Table.new(
          block_type,
          all,
          value_map: value_map,
          singular_df_name: singular_table_name,
          plural_df_name: table_name
        )
      end


      #
      # A class level hash containing optionally defined column names for a data frame.
      # Instead of numeric or dynamic column names, you may explicitly define names for columns using the
      #  "#{singular_table_name}_column_names" method.
      #
      #  E.g.
      #
      #  class Foo
      #    include HasStatus
      #    status_column_names %i(review_status export_status)
      #  end
      #
      #  This names
      #   column 0 as 'review_status' and
      #   column 1 as 'export_status'.
      #  Now you can make queries like:
      #  * Foo.status.review_status
      #  * Foo.first.status.export_status
      #  * Foo.status[:review_status..:export_status]
      #  * Foo.status[43] # You can still use numeric column indices
      #
      define_method :df_column_names do
        @@column_names ||= {}
      end

      # The class level accessor
      define_method(:column_name_map){|for_table|
        df_column_names[for_table][self] if defined? df_column_names[for_table] rescue nil
      }

      # The attribute writer
      define_method("#{singular_table_name}_column_names") do |names|
        df_column_names[singular_table_name] ||= {}
        df_column_maps[singular_table_name] ||= {}
        df_column_names[singular_table_name][self] = names
        df_column_maps[singular_table_name][self] = names.map.with_index.to_h
      end


      #
      # A class level hash containing optionally defined column maps (these are usually simply a hash that responds to #[](column_name) and returns
      # a positive integer representing the corresponding column index.
      # These are defined using the
      # "#{singular_table_name}_column_maps" method.
      #
      # class Foo
      #   include HasCpuTemp
      #   cpu_temp_column_map Hash.new{ |columns, time|
      #     columns[time] = time.to_i # We store cpu temperatures at a 1 second granularity
      #   }
      # end
      #
      define_method :df_column_maps do
        @@column_maps ||= {}
      end

      # The attribute writer
      define_method("#{singular_table_name}_column_map") do |column_map|
        df_column_names[singular_table_name] = nil
        df_column_maps[singular_table_name] ||= {}
        df_column_maps[singular_table_name][self] = column_map
      end

      # The class level accessor
      define_method(:column_map){|for_table|
        df_column_maps[for_table][self] if defined? df_column_maps[for_table] rescue nil
      }

      #
      # A class level has containing optionally defined reverse column mappings (from a positive integer to a mapped column index/key)
      # This is only used for functions where we query indices based on values.
      # E.g
      #
      # class Foo
      #   include HasPrice
      #   column_map Hash.new{|columns, date|
      #    columns[date] = (date - Date.new(1970)).to_i
      #   }
      #   reverse_column_map{|columns, index|
      #     columns[index] = Date.new(1970) + index.month
      #   }
      # end
      #
      # # Show all dates between 2000 and 2010 where the total of all prices is > $500
      # Foo.prices.idx_where_sum_gte(Date.new(2000)...Date.new(2010), 500)
      #
      define_method :df_reverse_column_maps do
        @@reverse_column_maps ||= {}
      end

      # The attribute writer
      define_method("#{singular_table_name}_reverse_column_map"){|reverse_column_map|
        df_reverse_column_maps[singular_table_name] ||= {}
        df_reverse_column_maps[singular_table_name][self] = reverse_column_map
      }

      # The class level accessor
      define_method(:reverse_column_map){|for_table|
        df_reverse_column_maps[for_table] ||= {}
        df_reverse_column_maps[for_table][self] ||= column_map(for_table).invert if column_map(for_table)
      }

      #
      # See group_proxy.rb.
      # This makes a number of grouping/bucketing queries easier to express
      # for analytics across an entire table
      #
      define_method(:with_groups) do |*groups|
        GroupProxy.new(group(*groups))
      end

      #
      # If you use the include_#{table_name} function before executing any queries, you can
      # join the child AR rows with any number of columns and treat them as if they were all part of the same table.
      # These joined columns can be used to further refine your queries, perform groupings, counts .etc
      #
      # E.g.
      #
      # class Iris
      #   include HasDimension
      #   dimension_column_names %i(sepal_length sepal_width petal_length petal_width)
      # end
      #
      # Iris.where('sepal_length > ?', 4) # Error! (There is no column called sepal_length on the iris table)
      # Iris.include_dimensions(:sepal_length).where('sepal_length > ?', 4) # Works fine
      # Iris.include_dimension(:sepal_length, :petal_width).where('sepal_length > 3').select(:petal_width)
      # Iris.include_dimension(:sepal_length, :petal_width).with_groups('ROUND(sepal_length)').average('petal_width')
      # {
      #   "4.0":"0.2"
      #   "5.0":"0.397872340425532",
      #   "6.0":"1.49705882352941",
      #   "7.0":"1.89583333333333",
      #   "8.0":"2.15",
      # }
      #
      # In cases where column names are not predefined or use a mapper you can provide a hash to give alternate column names for the query
      #
      # class BuildingType < ApplicationRecord
      #   include HasBuildingConsent
      #   consents_column_map Hash.new{|hash, time, as_date = time.to_date|
      #     (as_date.year - 1970) * 12 + as_date.month
      #   }
      # end
      #
      # # In this example BuildingType.consents accepts dynamic column indices (anything that responds to to_date)
      # # We can give these columns explicit names so we can refer to them in queries.
      # E.g
      #
      # BuildingType.include_consents({'1994-04-01' => april_94, '1994-05-01' => may_94}).where('april_94 + may_94 < 300')
      # => [
      #    <BuildingType id: 2, name: "Hostels_boarding", created_at: "2018-01-25 03:28:41", updated_at: "2018-01-25 03:28:41", data_frame_type: "BuildingType", data_frame_id: 2, april_94: 11, may_94: 5>,
      #    <BuildingType id: 3, name: "Hotels", created_at: "2018-01-25 03:28:41", updated_at: "2018-01-25 03:28:41", data_frame_type: "BuildingType", data_frame_id: 3, april_94: 33, may_94: 34>,
      #    <BuildingType id: 4, name: "Hospitals", created_at: "2018-01-25 03:28:41", updated_at: "2018-01-25 03:28:41", data_frame_type: "BuildingType", data_frame_id: 4, april_94: 32, may_94: 37>,
      #    <BuildingType id: 5, name: "Education", created_at: "2018-01-25 03:28:41", updated_at: "2018-01-25 03:28:41", data_frame_type: "BuildingType", data_frame_id: 5, april_94: 88, may_94: 145>,
      #    <BuildingType id: 6, name: "Social_cultural_religious", created_at: "2018-01-25 03:28:41", updated_at: "2018-01-25 03:28:41", data_frame_type: "BuildingType", data_frame_id: 6, april_94: 82, may_94: 102>,
      #    <BuildingType id: 9, name: "Storage", created_at: "2018-01-25 03:28:41", updated_at: "2018-01-25 03:28:41", data_frame_type: "BuildingType", data_frame_id: 9, april_94: 29, may_94: 52>,
      #    <BuildingType id: 12, name: "Misc", created_at: "2018-01-25 03:28:41", updated_at: "2018-01-25 03:28:41", data_frame_type: "BuildingType", data_frame_id: 12, april_94: 33, may_94: 39>]
      # ]
      #
      #
      define_method("include_#{table_name}"){|*dimensions, unmap: true, scope: self.all, as: false|
        dim1 = dimensions[0]
        case dim1
        when Hash
          dimension_map, dimensions = dim1, dim1.keys
        when Range
          exclude_end = dim1.exclude_end?

          from, to = if unmap && column_map(singular_table_name)
            unmap = false
            [column_map(singular_table_name)[dim1.begin],column_map(singular_table_name)[dim1.end]]
          else
            [dim1.begin, dim1.end]
          end
          dimensions = (exclude_end ? (from...to) : (from..to)).to_a
        end

        blocks_for_tables = scope.instance_eval{ @blocks_for_tables ||= {} }
        included_blocks   = blocks_for_tables[block_type.table_name]  ||= {}

        dimensions.flatten.each.with_index(1) do |key, i|
          if unmap && column_map(singular_table_name)
            idx = column_map(singular_table_name)[key]
            key = dimension_map[key] if dimension_map
          else
            idx = key
            key = "t#{key}"
          end
          key = "#{as}#{i}" if as
          block_index  = idx / block_type::BLOCK_SIZE
          block_offset = (idx % block_type::BLOCK_SIZE).succ
          included_blocks[block_index] ||= []
          included_blocks[block_index] << {name: key, idx: block_offset}
        end
        query = "(SELECT * FROM #{self.table_name} " + blocks_for_tables.reduce('') do |aggregate, (for_table, blocks_for_table)|
          aggregate +
            blocks_for_table.reduce('') do |blocks_aggregate, (block_idx, blocks)|
              blocks_table_name = for_table
              blocks_aggregate + " LEFT JOIN(SELECT #{blocks_table_name}.data_frame_type, #{blocks_table_name}.data_frame_id, " + blocks.map{|block| "#{blocks_table_name}.t#{block[:idx]} as \"#{block[:name]}\""}.join(', ') + " FROM #{blocks_table_name} "+
              " WHERE #{blocks_table_name}.period_index = #{block_idx}"+") b#{for_table}#{block_idx} ON b#{for_table}#{block_idx}.data_frame_type = '#{self.name}' AND b#{for_table}#{block_idx}.data_frame_id = #{self.table_name}.id"
            end
        end + ") as #{self.table_name}"
        scope.from(query)
      }
    end
  end
end