require 'active_support/concern'


module ActiveDataFrame
  def self.HasDataFrame(singular_table_name, table_name, block_type)
    to_inject = Module.new
    to_inject.extend ActiveSupport::Concern
    to_inject.included do
      define_method(singular_table_name){
        @data_frame_proxy ||= Row.new(block_type, self.class, self)
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

      define_method(table_name){
        Table.new(block_type, self)
      }

      define_method("where_#{table_name}") do |*args|
        Table.new(block_type, self).where(*args)
      end

      define_method("sum_#{table_name}") do |*args|
        Table.new(block_type, self).ar_sum(*args)
      end

      define_method("average_#{table_name}") do |*args|
        Table.new(block_type, self).ar_average(*args)
      end

      define_method("minimum_#{table_name}") do |*args|
        Table.new(block_type, self).ar_minimum(*args)
      end

      define_method("maximum_#{table_name}") do |*args|
        Table.new(block_type, self).ar_maximum(*args)
      end

      define_method("#{singular_table_name}_column_names") do |names|
        df_column_names[singular_table_name] ||= {}
        df_column_maps[singular_table_name] ||= {}
        df_column_names[singular_table_name][self] = names
        df_column_maps[singular_table_name][self] = names.map.with_index.to_h
      end

      define_method("#{singular_table_name}_column_map=") do |column_map|
        df_column_names[singular_table_name] = nil
        df_column_maps[singular_table_name] ||= {}
        df_column_maps[singular_table_name][self] = column_map
      end

      define_method(:column_map){|table_name|
        df_column_maps[table_name][self] if defined? df_column_maps[table_name]
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