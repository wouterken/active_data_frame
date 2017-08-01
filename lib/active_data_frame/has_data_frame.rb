require 'active_support/concern'


module ActiveDataFrame
  class GroupProxy
    attr_accessor :groups
    def initialize(groups)
      self.groups = groups
    end

    def min(column_name)
      aggregate('minimum', column_name)
    end

    def max(column_name)
      aggregate('maximum', column_name)
    end

    def sum(column_name)
      aggregate('sum', column_name)
    end

    def average(column_name)
      aggregate('average', column_name)
    end

    def count
      aggregate('count')
    end

    private
      def aggregate *agg
        counts = self.groups.send(*agg)
        grouped = {}
        counts.each do |keys, value|
          keys = Array(keys)
          child = keys[0..-2].reduce(grouped){|parent, key| parent[key] ||= {}}
          child[keys[-1]] = value
        end
        grouped
      end
  end

  def self.HasDataFrame(singular_table_name, table_name, block_type)
    to_inject = Module.new
    to_inject.extend ActiveSupport::Concern
    to_inject.included do
      define_method(singular_table_name){
        @data_frame_proxies ||= {}
        @data_frame_proxies[singular_table_name] ||= Row.new(block_type, self.class, self)
      }

      define_method(:inspect){
        inspection = "not initialized"
        if defined?(@attributes) && @attributes
           inspection = @attributes.keys.collect { |name|
             if has_attribute?(name)
               "#{name}: #{attribute_for_inspect(name)}"
             end
           }.compact.join(", ")
        end
        "<#{self.class} #{inspection}>"
      }
    end

    to_inject.class_methods do
      define_method(:df_column_names){
        @@column_names ||= {}
      }

      define_method(:df_column_maps){
        @@column_maps ||= {}
      }

      define_method(:df_reverse_column_maps){
        @@reverse_column_maps ||= {}
      }

      define_method(:with_groups){|*groups|
        GroupProxy.new(group(*groups))
      }

      define_method(table_name){
        Table.new(block_type, all)
      }

      define_method("include_#{table_name}"){|*dimensions, unmap: true|
        scope = self.all
        blocks_for_tables = scope.instance_eval{ @blocks_for_tables ||= {} }
        included_blocks = blocks_for_tables[singular_table_name] ||= {}
        dimensions.flatten.each do |key|
          if unmap && column_map(singular_table_name)
            idx = column_map(singular_table_name)[key]
          else
            idx = key
            key = "t#{key}"
          end
          block_index  = idx / block_type::BLOCK_SIZE
          block_offset = (idx % block_type::BLOCK_SIZE).succ
          included_blocks[block_index] ||= []
          included_blocks[block_index] << {name: key, idx: block_offset}
        end
        query = "(SELECT * FROM #{self.table_name} " + blocks_for_tables.reduce('') do |aggregate, (table_name, included_blocks)|
        aggregate +
          included_blocks.reduce('') do |aggregate, (block_idx, blocks)|
            blocks_table_name = "#{table_name}_blocks"
            aggregate + " LEFT JOIN(SELECT #{blocks_table_name}.data_frame_type, #{blocks_table_name}.data_frame_id, " + blocks.map{|block| "#{blocks_table_name}.t#{block[:idx]} as \"#{block[:name]}\""}.join(', ') + " FROM #{table_name}_blocks "+
            " WHERE #{blocks_table_name}.period_index = #{block_idx}"+") b#{table_name}#{block_idx} ON b#{table_name}#{block_idx}.data_frame_type = '#{self.name}' AND b#{table_name}#{block_idx}.data_frame_id = #{self.table_name}.id"
          end
        end + ") as #{self.table_name}"
        scope.from(query)
      }

      define_method("#{singular_table_name}_column_names") do |names|
        df_column_names[singular_table_name] ||= {}
        df_column_maps[singular_table_name] ||= {}
        df_column_names[singular_table_name][self] = names
        df_column_maps[singular_table_name][self] = names.map.with_index.to_h
      end

      define_method("#{singular_table_name}_column_map") do |column_map|
        df_column_names[singular_table_name] = nil
        df_column_maps[singular_table_name] ||= {}
        df_column_maps[singular_table_name][self] = column_map
      end

      define_method("#{singular_table_name}_reverse_column_map"){|reverse_column_map|
        df_reverse_column_maps[singular_table_name] ||= {}
        df_reverse_column_maps[singular_table_name][self] = reverse_column_map
      }

      define_method(:include_data_blocks){|table_name, *args|
        send("include_#{table_name}", *args)
      }

      define_method(:column_map){|table_name|
        df_column_maps[table_name][self] if defined? df_column_maps[table_name] rescue nil
      }

      define_method(:column_name_map){|table_name|
        df_column_names[table_name][self] if defined? df_column_names[table_name]
      }

      define_method(:reverse_column_map){|table_name|
        df_reverse_column_maps[table_name] ||= {}
        df_reverse_column_maps[table_name][self] ||= column_map(table_name).invert if column_map(table_name)
      }
    end

    return to_inject
  end
end