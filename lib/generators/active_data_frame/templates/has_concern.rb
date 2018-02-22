module <%= concern_name %>
  include ActiveDataFrame::HasDataFrame('<%= singular_table_name %>', Blocks::<%= block_type %>)
end