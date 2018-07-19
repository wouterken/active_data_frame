require 'rails/generators/active_record'

module ActiveDataFrame
  class InstallGenerator < ActiveRecord::Generators::Base
    STREAM_TYPES = %w(bit byte integer long float double)
    # Commandline options can be defined here using Thor-like options:
    argument :type,     type: :string,  default: 'float', desc: "DataFrame type. One of(#{STREAM_TYPES*" ,"})"
    argument :columns,  type: :numeric, default: 512,     desc: "Number of columns"
    argument :inject,   type: :array,   default: []

    def self.source_root
      @source_root ||= File.join(File.dirname(__FILE__), 'templates')
    end

    def generate_model
      invoke "active_record:model", ["blocks/#{singular_block_table_name}"], migration: false
    end

    def block_type
      "#{singular_table_name}_block".camelize
    end

    def block_table_name
      "#{singular_table_name}_blocks"
    end

    def singular_block_table_name
      "#{singular_table_name}_block"
    end

    def concern_name
      "Has#{singular_table_name.camelize}"
    end

    def concern_file_name
      "has_#{singular_table_name}"
    end

    def inject_concern_content
      inject.each do |inject_into|
        content = "  include #{concern_name}\n"
        class_name = inject_into.camelize
        inject_into_class(self.class.path_for_model(inject_into), class_name, content) if self.class.model_exists?(inject_into, destination_root)
      end
    end

    def get_typecode
      case type
      when "float", "double" then M::Typecode::FLOAT
      when "integer", "long" then M::Typecode::INT
      when "bit", "byte"     then M::Typecode::BYTE
      end
    end

    def inject_data_frame_helpers
      content = \
<<RUBY
  BLOCK_SIZE = #{columns}
  COLUMNS = %w(#{columns.times.map{|i| "t#{i+1}" }.join(" ")})
  TYPECODE = #{get_typecode}
  self.table_name = '#{block_table_name}'
RUBY
      class_name = "Blocks::#{singular_block_table_name.camelize}"
      inject_into_class(self.class.path_for_model(singular_block_table_name), class_name, content) if self.class.model_exists?(singular_block_table_name, destination_root)
    end

    def copy_concern
      template "has_concern.rb", "app/models/concerns/#{concern_file_name}.rb"
    end

    def self.path_for_model(model)
      File.join("app", "models", "blocks", "#{model.underscore}.rb")
    end

    def self.model_exists?(model, destination_root)
      File.exist?(File.join(destination_root, self.path_for_model(model)))
    end

    def copy_migration
      migration_template "migration.rb", "db/migrate/active_data_frame_create_#{table_name}.rb", migration_version: migration_version
    end

    def migration_data
<<RUBY
      t.integer :data_frame_id
      t.string  :data_frame_type
      t.integer :period_index
#{
    columns.times.map do |i|
"      t.#{type} :t#{i+1}, default: 0, allow_nil: false"
    end.join("\n")
    }
RUBY
    end

    def migration_version
      if rails5?
       "[#{Rails::VERSION::MAJOR}.#{Rails::VERSION::MINOR}]"
      end
    end

    def rails5?
      Rails.version.start_with? '5'
    end

  end
end