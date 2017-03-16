require 'rails/generators/active_record'

module ActiveDataFrame
  class InstallGenerator < ActiveRecord::Generators::Base
    desc "Generates a new data_frame type"

    STREAM_TYPES = %w(bit byte int long float double)
    # Commandline options can be defined here using Thor-like options:
    argument :type,    :type => :string, :default => 'float', :desc => "DataFrame type. One of(#{STREAM_TYPES*" ,"})"
    argument :columns, :type => :numeric, :default => 512, :desc => "Number of columns"
    argument :inject,     type: :array, default: []

    def self.source_root
      @source_root ||= File.join(File.dirname(__FILE__), 'templates')
    end

    def generate_model
      invoke "active_record:model", [singular_block_table_name], migration: false
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

    def inject_data_frame_helpers
      content = <<RUBY
  BLOCK_SIZE=#{columns}
  COLUMNS=%w(#{columns.times.map{|i| "t#{i+1}" }.join(" ")})
RUBY
      class_name = singular_block_table_name.camelize
      inject_into_class(self.class.path_for_model(singular_block_table_name), class_name, content) if self.class.model_exists?(singular_block_table_name, destination_root)
    end

    def copy_concern
      template "has_concern.rb", "app/models/concerns/#{concern_file_name}.rb"
    end

    def self.path_for_model(model)
      File.join("app", "models", "#{model.underscore}.rb")
    end

    def self.model_exists?(model, destination_root)
      File.exist?(File.join(destination_root, self.path_for_model(model)))
    end

    def copy_migration
      migration_template "migration.rb", "db/migrate/active_data_frame_create_#{table_name}.rb", migration_version: migration_version
    end


#     def generate_model
#       invoke "active_record:model", [name], migration: false unless model_exists? && behavior == :invoke
#     end

#     def inject_devise_content
#       content = model_contents

#       class_path = if namespaced?
#         class_name.to_s.split("::")
#       else
#         [class_name]
#       end

#       indent_depth = class_path.size - 1
#       content = content.split("\n").map { |line| "  " * indent_depth + line } .join("\n") << "\n"

#       inject_into_class(model_path, class_path.last, content) if model_exists?
#     end

#     def migration_data
# <<RUBY
#     ## Database authenticatable
#     t.string :email,              null: false, default: ""
#     t.string :encrypted_password, null: false, default: ""

#     ## Recoverable
#     t.string   :reset_password_token
#     t.datetime :reset_password_sent_at

#     ## Rememberable
#     t.datetime :remember_created_at

#     ## Trackable
#     t.integer  :sign_in_count, default: 0, null: false
#     t.datetime :current_sign_in_at
#     t.datetime :last_sign_in_at
#     t.#{ip_column} :current_sign_in_ip
#     t.#{ip_column} :last_sign_in_ip

#     ## Confirmable
#     # t.string   :confirmation_token
#     # t.datetime :confirmed_at
#     # t.datetime :confirmation_sent_at
#     # t.string   :unconfirmed_email # Only if using reconfirmable

#     ## Lockable
#     # t.integer  :failed_attempts, default: 0, null: false # Only if lock strategy is :failed_attempts
#     # t.string   :unlock_token # Only if unlock strategy is :email or :both
#     # t.datetime :locked_at
# RUBY
#     end

#     def ip_column
#       # Padded with spaces so it aligns nicely with the rest of the columns.
#       "%-8s" % (inet? ? "inet" : "string")
#     end

#     def inet?
#       postgresql?
#     end

#     def rails5?
#       Rails.version.start_with? '5'
#     end

#     def postgresql?
#       config = ActiveRecord::Base.configurations[Rails.env]
#       config && config['adapter'] == 'postgresql'
#     end

    def migration_data
<<RUBY
      t.integer :data_frame_id, index: true
      t.string  :data_frame_type, index: true
      t.integer :period_index, index: true
#{
    columns.times.map do |i|
"      t.#{type} :t#{i+1}"
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