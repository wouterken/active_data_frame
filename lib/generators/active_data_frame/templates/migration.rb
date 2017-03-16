class ActiveDataFrameCreate<%= table_name.camelize %> < ActiveRecord::Migration<%= migration_version %>
  def change
    create_table :<%= block_table_name %> do |t|
<%= migration_data -%>
      t.timestamps null: false
    end


    add_index :<%= block_table_name %>, [:data_frame_type, :data_frame_id , :period_index], :unique => true, name: 'index_<%= block_table_name %>_on_type_id_and_index'
  end
end
