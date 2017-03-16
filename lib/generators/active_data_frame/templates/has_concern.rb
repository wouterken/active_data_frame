require 'active_support/concern'

module <%= concern_name %>
  extend ActiveSupport::Concern
  include ActiveDataFrame::HasDataFrame('<%= singular_table_name %>', '<%= table_name %>',<%= block_type %>)
end